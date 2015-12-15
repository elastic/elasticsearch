/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.vagrant

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.*
import org.gradle.logging.ProgressLogger
import org.gradle.logging.ProgressLoggerFactory
import org.gradle.process.internal.ExecAction
import org.gradle.process.internal.ExecActionFactory

import javax.inject.Inject

/**
 * Runs bats over vagrant. Pretty much like running it using Exec but with a
 * nicer output formatter.
 */
class BatsOverVagrantTask extends DefaultTask {
  String command
  String boxName
  ExecAction execAction

  BatsOverVagrantTask() {
    execAction = getExecActionFactory().newExecAction()
  }

  @Inject
  ProgressLoggerFactory getProgressLoggerFactory() {
    throw new UnsupportedOperationException();
  }

  @Inject
  ExecActionFactory getExecActionFactory() {
    throw new UnsupportedOperationException();
  }

  void boxName(String boxName) {
    this.boxName = boxName
  }

  void command(String command) {
    this.command = command
  }

  @TaskAction
  void exec() {
    // It'd be nice if --machine-readable were, well, nice
    execAction.commandLine(['vagrant', 'ssh', boxName, '--command', command])
    execAction.setStandardOutput(new TapLoggerOutputStream(
      command: command,
      factory: getProgressLoggerFactory(),
      logger: logger))
    execAction.execute();
  }
}
