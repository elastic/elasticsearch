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

package org.elasticsearch.tools.launchers;

import com.sun.management.OperatingSystemMXBean;
import org.elasticsearch.tools.java_version_checker.JavaVersion;
import org.elasticsearch.tools.java_version_checker.SuppressForbidden;

import java.lang.management.ManagementFactory;

/**
 * A {@link SystemMemoryInfo} which delegates to {@link OperatingSystemMXBean}.
 *
 * <p>Prior to JDK 14 {@link OperatingSystemMXBean} did not take into consideration container memory limits when reporting total system
 * memory. Therefore attempts to use this implementation on earlier JDKs will result in an {@link SystemMemoryInfoException}.
 */
@SuppressForbidden(reason = "Using com.sun internals is the only way to query total system memory")
public final class DefaultSystemMemoryInfo implements SystemMemoryInfo {
    private final OperatingSystemMXBean operatingSystemMXBean;

    public DefaultSystemMemoryInfo() {
        this.operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    }

    @Override
    @SuppressWarnings("deprecation")
    public long availableSystemMemory() throws SystemMemoryInfoException {
        if (JavaVersion.majorVersion(JavaVersion.CURRENT) < 14) {
            throw new SystemMemoryInfoException("The minimum required Java version is 14 to use " + this.getClass().getName());
        }

        return operatingSystemMXBean.getTotalPhysicalMemorySize();
    }
}
