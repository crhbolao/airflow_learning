#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
SequentialExecutor

.. seealso::
    For more information on how the SequentialExecutor works, take a look at the guide:
    :ref:`executor:SequentialExecutor`
"""
import subprocess
from typing import Any, Optional

from airflow.executors.base_executor import BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils.state import State


class SequentialExecutor(BaseExecutor):
    """
    This executor will only run one task instance at a time, can be used
    for debugging. It is also the only executor that can be used with sqlite
    since sqlite doesn't support multiple connections.

    Since we want airflow to work out of the box, it defaults to this
    SequentialExecutor alongside sqlite as you first install it.
    """

    def __init__(self):
        super().__init__()
        self.commands_to_run = []

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: Optional[str] = None,
        executor_config: Optional[Any] = None,
    ) -> None:
        # 校验 command, 判断command的前三个是否为：["airflow", "tasks", "run"]
        self.validate_command(command)
        # 将command添加到commands_to_run中。
        self.commands_to_run.append((key, command))

    def sync(self) -> None:
        # 遍历commands_to_run，并依次调用子进程执行。所以sequentialExecutor是在单进程中依次执行。
        for key, command in self.commands_to_run:
            self.log.info("Executing command: %s", command)

            try:
                subprocess.check_call(command, close_fds=True)
                self.change_state(key, State.SUCCESS)
            except subprocess.CalledProcessError as e:
                self.change_state(key, State.FAILED)
                self.log.error("Failed to execute task %s.", str(e))

        self.commands_to_run = []

    def end(self):
        """End the executor."""
        self.heartbeat()

    def terminate(self):
        """Terminate the executor is not doing anything."""
