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
package org.elasticsearch.gradle

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

/**
 * Creates a file and sets it contents to something.
 */
class FileContentsTask extends DefaultTask {
  /**
   * The file to be built. Must be of type File to make @OutputFile happy.
   */
  @OutputFile
  File file

  @Input
  Object contents

  /**
   * The file to be built. Takes any objecct and coerces to a file.
   */
  void setFile(Object file) {
    this.file = file as File
  }

  @TaskAction
  void setContents() {
    file = file as File
    file.text = contents.toString()
  }
}
