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
import org.gradle.api.tasks.*
import org.gradle.internal.nativeintegration.filesystem.Chmod
import java.io.File
import javax.inject.Inject

/**
 * Creates an empty directory.
 */
class EmptyDirTask extends DefaultTask {
  @Input
  Object dir

  @Input
  int dirMode = 0755

  @TaskAction
  void create() {
    dir = dir as File
    dir.mkdirs()
    getChmod().chmod(dir, dirMode)
  }

  @Inject
  Chmod getChmod() {
    throw new UnsupportedOperationException()
  }
}
