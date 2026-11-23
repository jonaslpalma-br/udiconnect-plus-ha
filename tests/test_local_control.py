"""Tests for the control modes: cloud only, local with fallback, local only."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, patch

import aiohttp
from homeassistant.components.cover import (
    DOMAIN as COVER_DOMAIN,
    SERVICE_CLOSE_COVER,
    SERVICE_SET_COVER_POSITION,
    SERVICE_STOP_COVER,
)
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import ATTR_ENTITY_ID, STATE_UNAVAILABLE
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.udiconnect_plus.const import (
    CONF_CONTROL_MODE,
    MODE_CLOUD_ONLY,
    MODE_LOCAL_FALLBACK,
    MODE_LOCAL_ONLY,
)
from custom_components.udiconnect_plus.local import CommandResult, UdiLocalManager

from .conftest import SET_POSITION_URL, CloudMock, setup_integration

SALA = "cover.persiana_sala"
MAC = "aa:bb:cc:dd:ee:01"

_MANAGER = "custom_components.udiconnect_plus.coordinator.UdiLocalManager"


def _with_mode(entry: MockConfigEntry, hass: HomeAssistant, mode: str) -> None:
    entry.add_to_hass(hass)
    hass.config_entries.async_update_entry(entry, options={CONF_CONTROL_MODE: mode})


async def test_cloud_only_opens_nothing_local(
    hass: HomeAssistant, cloud: CloudMock, mock_config_entry: MockConfigEntry
) -> None:
    """The default mode behaves as before: no socket, no saved list."""
    assert CONF_CONTROL_MODE not in mock_config_entry.options
    with patch(f"{_MANAGER}.async_start", new=AsyncMock()) as start:
        assert await setup_integration(hass, mock_config_entry)

    start.assert_not_called()
    coordinator = mock_config_entry.runtime_data
    assert coordinator.control_mode == MODE_CLOUD_ONLY
    assert not coordinator.local_enabled
    assert not coordinator.local_manager.listening


@pytest.mark.parametrize("mode", [MODE_LOCAL_FALLBACK, MODE_LOCAL_ONLY])
async def test_local_modes_start_the_listener(
    hass: HomeAssistant,
    cloud: CloudMock,
    mock_config_entry: MockConfigEntry,
    mode: str,
) -> None:
    _with_mode(mock_config_entry, hass, mode)
    with patch(f"{_MANAGER}.async_start", new=AsyncMock()) as start:
        assert await setup_integration(hass, mock_config_entry)

    start.assert_awaited_once()
    assert mock_config_entry.runtime_data.local_enabled


@pytest.mark.parametrize(
    ("service", "data", "method", "args"),
    [
        (SERVICE_STOP_COVER, {}, "async_stop", (MAC,)),
        (SERVICE_CLOSE_COVER, {}, "async_set_position", (MAC, 0)),
        (SERVICE_SET_COVER_POSITION, {"position": 35}, "async_set_position", (MAC, 35)),
    ],
)
async def test_confirmed_command_skips_the_cloud(
    hass: HomeAssistant,
    cloud: CloudMock,
    mock_config_entry: MockConfigEntry,
    service,
    data,
    method,
    args,
) -> None:
    _with_mode(mock_config_entry, hass, MODE_LOCAL_FALLBACK)
    with (
        patch(f"{_MANAGER}.async_start", new=AsyncMock()),
        patch(
            f"{_MANAGER}.{method}",
            new=AsyncMock(return_value=CommandResult(True, 1, 0.2)),
        ) as send,
    ):
        assert await setup_integration(hass, mock_config_entry)
        await hass.services.async_call(
            COVER_DOMAIN, service, {ATTR_ENTITY_ID: SALA, **data}, blocking=True
        )

    send.assert_awaited_once_with(*args)
    assert cloud.calls_to(SET_POSITION_URL) == []


async def test_unconfirmed_command_falls_back_to_the_cloud(
    hass: HomeAssistant,
    cloud: CloudMock,
    mock_config_entry: MockConfigEntry,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _with_mode(mock_config_entry, hass, MODE_LOCAL_FALLBACK)
    with (
        patch(f"{_MANAGER}.async_start", new=AsyncMock()),
        patch(
            f"{_MANAGER}.async_set_position",
            new=AsyncMock(return_value=CommandResult(False, 2, 3.0)),
        ),
        caplog.at_level(logging.WARNING),
    ):
        assert await setup_integration(hass, mock_config_entry)
        await hass.services.async_call(
            COVER_DOMAIN,
            SERVICE_CLOSE_COVER,
            {ATTR_ENTITY_ID: SALA},
            blocking=True,
        )

    calls = cloud.calls_to(SET_POSITION_URL)
    assert len(calls) == 1
    assert calls[0][2]["position"] == 0
    assert "not confirmed over the LAN" in caplog.text


async def test_local_only_never_uses_the_cloud(
    hass: HomeAssistant, cloud: CloudMock, mock_config_entry: MockConfigEntry
) -> None:
    _with_mode(mock_config_entry, hass, MODE_LOCAL_ONLY)
    with (
        patch(f"{_MANAGER}.async_start", new=AsyncMock()),
        patch(
            f"{_MANAGER}.async_set_position",
            new=AsyncMock(return_value=CommandResult(False, 2, 3.0)),
        ),
    ):
        assert await setup_integration(hass, mock_config_entry)
        with pytest.raises(HomeAssistantError, match="did not confirm"):
            await hass.services.async_call(
                COVER_DOMAIN,
                SERVICE_CLOSE_COVER,
                {ATTR_ENTITY_ID: SALA},
                blocking=True,
            )

    assert cloud.calls_to(SET_POSITION_URL) == []


async def test_cloud_only_ignores_the_lan(
    hass: HomeAssistant, cloud: CloudMock, mock_config_entry: MockConfigEntry
) -> None:
    with (
        patch(f"{_MANAGER}.async_set_position", new=AsyncMock()) as local_move,
        patch.object(UdiLocalManager, "position", return_value=7),
    ):
        assert await setup_integration(hass, mock_config_entry)
        await hass.services.async_call(
            COVER_DOMAIN,
            SERVICE_CLOSE_COVER,
            {ATTR_ENTITY_ID: SALA},
            blocking=True,
        )

    local_move.assert_not_called()
    assert len(cloud.calls_to(SET_POSITION_URL)) == 1
    assert hass.states.get(SALA).attributes["current_position"] == 100


async def test_lan_position_wins_over_the_cloud_copy(
    hass: HomeAssistant, cloud: CloudMock, mock_config_entry: MockConfigEntry
) -> None:
    _with_mode(mock_config_entry, hass, MODE_LOCAL_FALLBACK)
    with (
        patch(f"{_MANAGER}.async_start", new=AsyncMock()),
        patch.object(UdiLocalManager, "position", return_value=42),
    ):
        assert await setup_integration(hass, mock_config_entry)

    assert hass.states.get(SALA).attributes["current_position"] == 42


async def test_starts_from_the_saved_list_when_the_cloud_is_down(
    hass: HomeAssistant,
    cloud: CloudMock,
    mock_config_entry: MockConfigEntry,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _with_mode(mock_config_entry, hass, MODE_LOCAL_FALLBACK)
    with patch(f"{_MANAGER}.async_start", new=AsyncMock()):
        assert await setup_integration(hass, mock_config_entry)
        assert await hass.config_entries.async_unload(mock_config_entry.entry_id)

        cloud.sync_exc = aiohttp.ClientError("cloud down")
        with (
            patch.object(UdiLocalManager, "is_reachable", return_value=True),
            caplog.at_level(logging.WARNING),
        ):
            await hass.config_entries.async_setup(mock_config_entry.entry_id)
            await hass.async_block_till_done()

            assert mock_config_entry.state is ConfigEntryState.LOADED
            assert "starting from the saved list" in caplog.text
            # The motor answers on the LAN, so the cover is usable.
            assert hass.states.get(SALA).state != STATE_UNAVAILABLE


async def test_cloud_down_without_a_saved_list_still_retries(
    hass: HomeAssistant, cloud: CloudMock, mock_config_entry: MockConfigEntry
) -> None:
    cloud.sync_exc = aiohttp.ClientError("cloud down")
    mock_config_entry.add_to_hass(hass)
    await hass.config_entries.async_setup(mock_config_entry.entry_id)
    await hass.async_block_till_done()

    assert mock_config_entry.state is ConfigEntryState.SETUP_RETRY


async def test_changing_the_mode_reloads_the_entry(
    hass: HomeAssistant, cloud: CloudMock, mock_config_entry: MockConfigEntry
) -> None:
    assert await setup_integration(hass, mock_config_entry)
    first = mock_config_entry.runtime_data
    assert not first.local_enabled

    with patch(f"{_MANAGER}.async_start", new=AsyncMock()) as start:
        hass.config_entries.async_update_entry(
            mock_config_entry, options={CONF_CONTROL_MODE: MODE_LOCAL_FALLBACK}
        )
        await hass.async_block_till_done()

    assert mock_config_entry.runtime_data is not first
    assert mock_config_entry.runtime_data.local_enabled
    start.assert_awaited_once()
