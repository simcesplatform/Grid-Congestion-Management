
# Grid

Author:

> Mehdi Attar
>
> Tampere University
>
> Finland

**Introduction**

The Grid component perform backward forward power flow. The component is developed to be used in SimCES platform.

**Workflow of Grid**

1. Grid receives the [network ](https://simcesplatform.github.io/energy_msg-init-nis-networkbusinfo/)and [customer](https://simcesplatform.github.io/energy_msg-init-cis-customerinfo/) information data.
2. Grid receives [resource ](https://simcesplatform.github.io/energy_msg-resourcestate/)state data.
3. Grid performs backward forward sweep power flow.
4. Grid publishes the grid state variables (i.e., [voltage](https://simcesplatform.github.io/energy_msg-networkstate-voltage/) and [current](https://simcesplatform.github.io/energy_msg-networkstate-current/)).

**Epoch workflow**

In beginning of the simulation the Grid component will wait for [SimState](https://simcesplatform.github.io/core_msg-simstate/)(running) message, when the message is received component will initialize and send [Status](https://simcesplatform.github.io/core_msg-status/)(Ready) message with epoch number 0. If SimState(stopped) is received component will close down. Other message are ignored at this stage.

After startup component will begin to listen for [epoch](https://simcesplatform.github.io/core_msg-epoch/) messages. It waits until necessary input data including grid related data ([buses](https://simcesplatform.github.io/energy_msg-init-nis-networkbusinfo/), [components](https://simcesplatform.github.io/energy_msg-init-nis-networkcomponentinfo/) and [customer](https://simcesplatform.github.io/energy_msg-init-cis-customerinfo/)) in addition to [resource ](https://simcesplatform.github.io/energy_msg-resourcestate/)state data to arrive.

After receiving the necessary input data, the Grid calculates a power flow. The number of iterations in the power flow depends on the desired acuracy defined in the parametrization. Then grid state variables are published. Then the Grid component sends a [status](https://simcesplatform.github.io/core_msg-status/) ready message indicating that all the processes for the running epoch is finished and a new epoch can be started.

If at any stage of the execution Status (Error) message is received component will immediately close down

**Implementation details**

* Language and platform

| Programming language | Python 3.11.4                                             |
| -------------------- | ---------------------------------------------------------- |
| Operating system     | Docker version 20.10.21 running on windows 10 version 22H2 |

**External packages**

The following packages are needed.

| Package          | Version   | Why needed                                                                                | URL                                                                                                   |
| ---------------- | --------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Simulation Tools | (Unknown) | "Tools for working with simulation messages and with the RabbitMQ message bus in Python." | [https://github.com/simcesplatform/simulation-tools](https://github.com/simcesplatform/simulation-tools) |
