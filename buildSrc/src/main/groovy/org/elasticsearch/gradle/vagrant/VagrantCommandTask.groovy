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
 * Runs a vagrant command. Pretty much like Exec task but with a nicer output
 * formatter and defaults to `vagrant` as first part of commandLine.
 */
class VagrantCommandTask extends DefaultTask {
  List<Object> commandLine
  String boxName
  ExecAction execAction

  VagrantCommandTask() {
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

  void commandLine(Object... commandLine) {
    this.commandLine = commandLine
  }

  @TaskAction
  void exec() {
    // It'd be nice if --machine-readable were, well, nice
    execAction.commandLine(['vagrant'] + commandLine)
    execAction.setStandardOutput(new VagrantLoggerOutputStream(
      command: commandLine.join(' '),
      factory: getProgressLoggerFactory(),
      /* Vagrant tends to output a lot of stuff, but most of the important
        stuff starts with ==> $box */
      squashedPrefix: "==> $boxName: "))
    execAction.execute();
  }
}
