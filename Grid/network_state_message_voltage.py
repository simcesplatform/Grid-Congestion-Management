# -*- coding: utf-8 -*-
# Copyright 2023 Tampere University.
# This software was developed as a part of doctroal studies of Mehdi Attar, funded by Fortum and Neste Foundation.
#  This source code is licensed under the MIT license. See LICENSE in the repository root directory.
# Author(s): Mehdi Attar <mehdi.attar@tuni.fi>
#            software template : Ville Heikkilä <ville.heikkila@tuni.fi>
"""This module contains the message class for the simulation platform network state messages (voltage).
https://simcesplatform.github.io/energy_msg-networkstate-voltage/ """

from __future__ import annotations
from typing import Any, Dict, List, Union

from tools.exceptions.messages import MessageValueError
from tools.message.abstract import AbstractResultMessage, AbstractMessage
from tools.message.block import QuantityBlock
from tools.tools import FullLogger

LOGGER = FullLogger(__name__)


class NetworkStateMessageVoltage(AbstractResultMessage):
    """Class containing all the attributes for a Network State message."""

    # message type for these messages
    CLASS_MESSAGE_TYPE = "NetworkState"
    MESSAGE_TYPE_CHECK = True

    # Mapping from message JSON attributes to class attributes
    MESSAGE_ATTRIBUTES = {
        "Magnitude" : "magnitude",
        "Angle" : "angle",
        "Bus" : "bus",
        "Node": "node"
    }
    OPTIONAL_ATTRIBUTES = []

    # attributes whose value should be a QuantityBlock and the expected unit of measure.
    QUANTITY_BLOCK_ATTRIBUTES = {
        "Magnitude": "kV",
        "Angle": "deg"
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

    # allowed values for the node attribute
    ACCEPTED_NODE_VALUES = [1, 2, 3, "neutral"]

    @property
    def magnitude(self) -> QuantityBlock:
        """The attribute for magnitude of the voltage."""
        return self.__magnitude
    
    @magnitude.setter
    def magnitude(self, magnitude: Union[str, float, QuantityBlock, Dict[str, Any]]):
        """Set value for magnitude.
        A string value is converted to a float. A float value is converted into a QuantityBlock with the default unit.
        A dict is converted to a QuantityBlock.
        Raises MessageValueError if value is missing or invalid: a QuantityBlock has the wrong unit, dict cannot be converted  or
        a string cannot be converted to float"""
        if self._check_magnitude(magnitude):
            self._set_quantity_block_value('Magnitude', magnitude)
            return

        raise MessageValueError("'{}' is an invalid value for magnitude.".format(str(magnitude)))

    @classmethod
    def _check_magnitude(cls, magnitude: Union[str, float, QuantityBlock]) -> bool:
        """Check that the unit for magnitude is valid."""
        return cls._check_quantity_block(magnitude, cls.QUANTITY_BLOCK_ATTRIBUTES_FULL['Magnitude'])

    ##################################
    @property
    def angle(self) -> QuantityBlock:
        """The attribute for voltage's angle."""
        return self.__angle
    
    @angle.setter
    def angle(self, angle: Union[str, float, QuantityBlock, Dict[str, Any]]):
        """Set value for angle.
        A string value is converted to a float. A float value is converted into a QuantityBlock with the default unit.
        A dict is converted into a QuantityBlock.
        Raises MessageValueError if value is missing or invalid: a QuantityBlock has the wrong unit, dict cannot be converted  or
        a string cannot be converted to float"""
        if self._check_angle(angle):
            self._set_quantity_block_value('Angle', angle)
            return

        raise MessageValueError("'{}' is an invalid value for angle.".format(str(angle)))

    @classmethod
    def _check_angle(cls, angle: Union[str, float, QuantityBlock]) -> bool:
        """Check that the unit for angle is valid."""
        return cls._check_quantity_block(angle, cls.QUANTITY_BLOCK_ATTRIBUTES_FULL['Angle'])

    ##################################

    @property
    def bus(self) -> str:
        """The attribute for the name of bus to which the voltage is for."""
        return self.__bus
    
    @bus.setter
    def bus(self, bus: str):
        """Set value for bus."""
        if self._check_bus(bus):
            self.__bus = bus
            return

        raise MessageValueError(f"'{bus}' is an invalid value for bus since it is not a string.")

    @classmethod
    def _check_bus(cls, bus: str) -> bool:
        """Check that value for bus is valid i.e. a string."""
        return isinstance(bus, str)

    #################################

    @property
    def node(self) -> List[int,str]:
        return self.__node

    @node.setter
    def node(self, node: List[int,str]):
        if self._check_node(node):
            self.__node=node
        else:
            raise MessageValueError("Invalid value, {}, for attribute: node".format(node))

    @classmethod
    def _check_node(cls, node: List[int,str]) -> bool:
        LOGGER.info("node is {}".format(node))
        if node in cls.ACCEPTED_NODE_VALUES:
            return True
        else:
            return False


NetworkStateMessageVoltage.register_to_factory()
