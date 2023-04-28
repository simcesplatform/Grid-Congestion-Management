# -*- coding: utf-8 -*-
# Copyright 2023 Tampere University.
# This software was developed as a part of doctroal studies of Mehdi Attar, funded by Fortum and Neste Foundation.
#  This source code is licensed under the MIT license. See LICENSE in the repository root directory.
# Author(s): Mehdi Attar <mehdi.attar@tuni.fi>
#            software template : Ville Heikkilä <ville.heikkila@tuni.fi>
"""This module contains the message class for the simulation platform network state messages (current).
https://simcesplatform.github.io/energy_msg-networkstate-current/ """

from __future__ import annotations
from typing import Any, Dict, List, Union

from tools.exceptions.messages import MessageValueError
from tools.message.abstract import AbstractResultMessage, AbstractMessage
from tools.message.block import QuantityBlock
from tools.tools import FullLogger

LOGGER = FullLogger(__name__)


class NetworkStateMessageCurrent(AbstractResultMessage):
    """Class containing all the attributes for a Network State message."""

    # message type for these messages
    CLASS_MESSAGE_TYPE = "NetworkState"
    MESSAGE_TYPE_CHECK = True

    # Mapping from message JSON attributes to class attributes
    MESSAGE_ATTRIBUTES = {
        "MagnitudeSendingEnd" : "magnitude_sending_end", 
        "MagnitudeReceivingEnd" : "magnitude_receiving_end", 
        "AngleSendingEnd" : "angle_sending_end",  
        "AngleReceivingEnd" : "angle_receiving_end",  
        "DeviceId" : "device_id",  
        "Phase": "phase"
    }
    OPTIONAL_ATTRIBUTES = []

    # attributes whose value should be a QuantityBlock and the expected unit of measure.
    QUANTITY_BLOCK_ATTRIBUTES = {
        "MagnitudeSendingEnd": "A",
        "MagnitudeReceivingEnd": "A",
        "AngleSendingEnd": "deg",
        "AngleReceivingEnd": "deg"
    }

    MESSAGE_ATTRIBUTES_FULL = {
        **AbstractResultMessage.MESSAGE_ATTRIBUTES_FULL,
        **MESSAGE_ATTRIBUTES
    }
    OPTIONAL_ATTRIBUTES_FULL = AbstractResultMessage.OPTIONAL_ATTRIBUTES_FULL + OPTIONAL_ATTRIBUTES
    QUANTITY_BLOCK_ATTRIBUTES_FULL = {
        **AbstractMessage.QUANTITY_BLOCK_ATTRIBUTES_FULL,
        **QUANTITY_BLOCK_ATTRIBUTES
    }

    # allowed values for the phase attribute
    ACCEPTED_PHASE_VALUES = [1, 2, 3, "neutral"]

    @property
    def magnitude_sending_end(self) -> QuantityBlock:
        """The attribute for magnitude of the current."""
        return self.__magnitude_sending_end
    
    @magnitude_sending_end.setter
    def magnitude_sending_end(self, magnitude_sending_end: Union[str, float, QuantityBlock, Dict[str, Any]]):
        """Set value for current magnitude.
        A string value is converted to a float. A float value is converted into a QuantityBlock with the default unit.
        A dict is converted to a QuantityBlock.
        Raises MessageValueError if value is missing or invalid: a QuantityBlock has the wrong unit, dict cannot be converted  or
        a string cannot be converted to float"""
        if self._check_magnitude_sending_end(magnitude_sending_end):
            self._set_quantity_block_value('MagnitudeSendingEnd', magnitude_sending_end)
            return

        raise MessageValueError("'{}' is an invalid value for magnitude.".format(str(magnitude_sending_end)))

    @classmethod
    def _check_magnitude_sending_end(cls, magnitude_sending_end: Union[str, float, QuantityBlock]) -> bool:
        """Check that the unit for magnitude is valid."""
        return cls._check_quantity_block(magnitude_sending_end, cls.QUANTITY_BLOCK_ATTRIBUTES_FULL['MagnitudeSendingEnd'])

    ##################################

    @property
    def magnitude_receiving_end(self) -> QuantityBlock:
        """The attribute for magnitude of the current."""
        return self.__magnitude_receiving_end
    
    @magnitude_receiving_end.setter
    def magnitude_receiving_end(self, magnitude_receiving_end: Union[str, float, QuantityBlock, Dict[str, Any]]):
        """Set value for current magnitude.
        A string value is converted to a float. A float value is converted into a QuantityBlock with the default unit.
        A dict is converted to a QuantityBlock.
        Raises MessageValueError if value is missing or invalid: a QuantityBlock has the wrong unit, dict cannot be converted  or
        a string cannot be converted to float"""
        if self._check_magnitude_receiving_end(magnitude_receiving_end):
            self._set_quantity_block_value('MagnitudeReceivingEnd', magnitude_receiving_end)
            return

        raise MessageValueError("'{}' is an invalid value for magnitude.".format(str(magnitude_receiving_end)))

    @classmethod
    def _check_magnitude_receiving_end(cls, magnitude_receiving_end: Union[str, float, QuantityBlock]) -> bool:
        """Check that the unit for magnitude is valid."""
        return cls._check_quantity_block(magnitude_receiving_end, cls.QUANTITY_BLOCK_ATTRIBUTES_FULL['MagnitudeReceivingEnd'])


    ##################################

    @property
    def angle_sending_end(self) -> QuantityBlock:
        """The attribute for current's angle."""
        return self.__angle_sending_end
    
    @angle_sending_end.setter
    def angle_sending_end(self, angle_sending_end: Union[str, float, QuantityBlock, Dict[str, Any]]):
        """Set value for angle.
        A string value is converted to a float. A float value is converted into a QuantityBlock with the default unit.
        A dict is converted into a QuantityBlock.
        Raises MessageValueError if value is missing or invalid: a QuantityBlock has the wrong unit, dict cannot be converted  or
        a string cannot be converted to float"""
        if self._check_angle_sending_end(angle_sending_end):
            self._set_quantity_block_value('AngleSendingEnd', angle_sending_end)
            return

        raise MessageValueError("'{:s}' is an invalid value for angle.".format(str(angle_sending_end)))

    @classmethod
    def _check_angle_sending_end(cls, angle_sending_end: Union[str, float, QuantityBlock]) -> bool:
        """Check that the unit for angle is valid."""
        return cls._check_quantity_block(angle_sending_end, cls.QUANTITY_BLOCK_ATTRIBUTES_FULL['AngleSendingEnd'])

    ###################################

    @property
    def angle_receiving_end(self) -> QuantityBlock:
        """The attribute for current's angle."""
        return self.__angle_receiving_end
    
    @angle_receiving_end.setter
    def angle_receiving_end(self, angle_receiving_end: Union[str, float, QuantityBlock, Dict[str, Any]]):
        """Set value for angle.
        A string value is converted to a float. A float value is converted into a QuantityBlock with the default unit.
        A dict is converted into a QuantityBlock.
        Raises MessageValueError if value is missing or invalid: a QuantityBlock has the wrong unit, dict cannot be converted  or
        a string cannot be converted to float"""
        if self._check_angle_receiving_end(angle_receiving_end):
            self._set_quantity_block_value('AngleReceivingEnd', angle_receiving_end)
            return

        raise MessageValueError("'{:s}' is an invalid value for angle.".format(str(angle_receiving_end)))

    @classmethod
    def _check_angle_receiving_end(cls, angle_receiving_end: Union[str, float, QuantityBlock]) -> bool:
        """Check that the unit for angle is valid."""
        return cls._check_quantity_block(angle_receiving_end, cls.QUANTITY_BLOCK_ATTRIBUTES_FULL['AngleReceivingEnd'])

    ###################################

    @property
    def device_id(self) -> str:
        """The attribute for the name of component to which the current is for."""
        return self.__device_id
    
    @device_id.setter
    def device_id(self, device_id: str):
        """Set value for device."""
        if self._check_device_id(device_id):
            self.__device_id = device_id
            return

        raise MessageValueError(f"'{device_id}' is an invalid value for deviceid since it is not a string.")

    @classmethod
    def _check_device_id(cls, device_id: str) -> bool:
        """Check that value for bus is valid i.e. a string."""
        return isinstance(device_id, str)

    ###################################

    @property
    def phase(self) -> List[int,str]:
        return self.__phase

    @phase.setter
    def phase(self, phase: List[int,str]):
        if self._check_phase(phase):
            self.__phase = phase
        else:
            raise MessageValueError("Invalid value, {}, for attribute: phase".format(phase))

    @classmethod
    def _check_phase(cls, phase: List[int,str]) -> bool:
        LOGGER.info("phase is {}".format(phase))
        if phase in cls.ACCEPTED_PHASE_VALUES:
            return True
        else:
            return False


NetworkStateMessageCurrent.register_to_factory()
