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

package org.elasticsearch.transport.nio;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import static org.elasticsearch.common.settings.Setting.boolSetting;

public class NioNotEnabledBootstrapCheck implements BootstrapCheck {

    public static final Setting<Boolean> OVERRIDE_BOOTSTRAP =
        boolSetting("transport.nio.override_nio_bootstrap_check", false, Setting.Property.NodeScope);

    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        if (OVERRIDE_BOOTSTRAP.get(context.settings)) {
            return BootstrapCheckResult.success();
        } else {
            return BootstrapCheckResult.failure("The transport-nio plugin is experimental and not ready for production usage. It should " +
                "not be enabled in production.");
        }
    }
}
