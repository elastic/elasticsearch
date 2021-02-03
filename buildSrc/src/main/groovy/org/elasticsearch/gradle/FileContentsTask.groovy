/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
