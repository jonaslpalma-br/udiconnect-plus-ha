"""DataUpdateCoordinator for Udiconnect Plus."""

from __future__ import annotations

from datetime import timedelta
import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.debounce import Debouncer
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import (
    UdiconnectApiError,
    UdiconnectAuthError,
    UdiconnectClient,
    UdiconnectConnectionError,
    UdiDevice,
)
from .const import CONF_CONTROL_MODE, DEFAULT_CONTROL_MODE, DOMAIN, MODE_CLOUD_ONLY
from .local import UdiLocalManager

_LOGGER = logging.getLogger(__name__)

REQUEST_REFRESH_COOLDOWN = 2.0
STORAGE_VERSION = 1

# What identifies a device and almost never changes. Live values (position,
# online, battery) stay out: after a restart they would show as current.
_CACHED_FIELDS = (
    "unique_id",
    "device_id",
    "name",
    "home_id",
    "home_name",
    "category",
    "controller_type",
    "model",
    "firmware_version",
    "latest_firmware_version",
    "mac_address",
)

type UdiconnectConfigEntry = ConfigEntry[UdiconnectCoordinator]


def _device_store(hass: HomeAssistant, entry_id: str) -> Store[list[dict[str, Any]]]:
    return Store(hass, STORAGE_VERSION, f"{DOMAIN}.{entry_id}")


async def async_remove_device_cache(hass: HomeAssistant, entry_id: str) -> None:
    """Delete the saved device list of a removed entry."""
    await _device_store(hass, entry_id).async_remove()


def _snapshot(devices: dict[str, UdiDevice]) -> list[dict[str, Any]]:
    return [
        {name: getattr(device, name) for name in _CACHED_FIELDS}
        for device in devices.values()
    ]


class UdiconnectCoordinator(DataUpdateCoordinator[dict[str, UdiDevice]]):
    """Fetch device state from the cloud and manage local UDP engine."""

    config_entry: UdiconnectConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        entry: UdiconnectConfigEntry,
        client: UdiconnectClient,
        scan_interval: int,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            config_entry=entry,
            name=DOMAIN,
            update_interval=timedelta(seconds=scan_interval),
            request_refresh_debouncer=Debouncer(
                hass, _LOGGER, cooldown=REQUEST_REFRESH_COOLDOWN, immediate=False
            ),
        )
        self.client = client
        self.control_mode: str = entry.options.get(
            CONF_CONTROL_MODE, DEFAULT_CONTROL_MODE
        )
        # In cloud-only mode nothing local is opened, saved or read.
        self.local_enabled = self.control_mode != MODE_CLOUD_ONLY
        # The LAN state lives in the local manager, not in self.data: a cloud
        # poll would overwrite it, and async_set_updated_data would mark a
        # failing cloud as healthy. Entities read both and pick.
        self.local_manager = UdiLocalManager(self._async_local_changed)
        # Last device list from the cloud, so the entry can start and keep the
        # LAN control when the cloud is down at startup.
        self._store = _device_store(hass, entry.entry_id)
        self._saved: list[dict[str, Any]] | None = None

    async def async_load_device_cache(self) -> dict[str, UdiDevice]:
        """Read the saved device list. Empty if there is none or LAN is off."""
        if not self.local_enabled:
            return {}
        stored = await self._store.async_load()
        if not stored:
            return {}
        try:
            devices = {
                item["unique_id"]: UdiDevice(
                    **{name: item.get(name) for name in _CACHED_FIELDS},
                    position=None,
                    is_online=False,
                    is_calibrating=False,
                    low_battery=False,
                    new_firmware_available=False,
                )
                for item in stored
            }
        except (KeyError, TypeError) as err:
            _LOGGER.warning("Ignoring the saved device list: %s", err)
            return {}
        self._saved = stored
        return devices

    @callback
    def _async_local_changed(self) -> None:
        """Refresh the entities when a module moves, appears or returns on the LAN."""
        if self.data is not None:
            self.async_update_listeners()

    async def _async_update_data(self) -> dict[str, UdiDevice]:
        try:
            devices = await self.client.async_get_devices()
        except UdiconnectAuthError as err:
            raise ConfigEntryAuthFailed(
                translation_domain=DOMAIN,
                translation_key="invalid_auth",
            ) from err
        except UdiconnectConnectionError as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="cannot_connect",
                translation_placeholders={"error": str(err)},
            ) from err
        except UdiconnectApiError as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="unexpected_response",
                translation_placeholders={"error": str(err)},
            ) from err

        if self.data is not None:
            for unique_id in devices.keys() - self.data.keys():
                device = devices[unique_id]
                _LOGGER.info("New device found: %s (%s)", device.name, device.device_id)

        if self.local_enabled:
            snapshot = _snapshot(devices)
            if snapshot != self._saved:
                await self._store.async_save(snapshot)
                self._saved = snapshot
        return devices
