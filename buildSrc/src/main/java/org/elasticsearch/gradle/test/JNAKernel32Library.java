/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.test;

import com.sun.jna.Native;
import com.sun.jna.WString;
import org.apache.tools.ant.taskdefs.condition.Os;

public class JNAKernel32Library {

    private static final class Holder {
        private static final JNAKernel32Library instance = new JNAKernel32Library();
    }

    static JNAKernel32Library getInstance() {
        return Holder.instance;
    }

    private JNAKernel32Library() {
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            Native.register("kernel32");
        }
    }

    native int GetShortPathNameW(WString lpszLongPath, char[] lpszShortPath, int cchBuffer);

}
