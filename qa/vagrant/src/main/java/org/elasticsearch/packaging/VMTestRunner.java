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

package org.elasticsearch.packaging;

import org.junit.runner.JUnitCore;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Ensures that the current JVM is running on a virtual machine before delegating to {@link JUnitCore}. We just check for the existence
 * of a special file that we create during VM provisioning.
 */
public class VMTestRunner {
    public static void main(String[] args) {
        if (Files.exists(Paths.get("/is_vagrant_vm"))) {
            JUnitCore.main(args);
        } else {
            throw new RuntimeException("This filesystem does not have an expected marker file indicating it's a virtual machine. These " +
                "tests should only run in a virtual machine because they're destructive.");
        }
    }
}
