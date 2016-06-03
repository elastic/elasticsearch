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

package org.elasticsearch.cloud.gce;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;

import java.io.IOException;
import java.util.function.Function;

/**
 * Access Google Compute Engine metadata
 */
public interface GceMetadataService extends LifecycleComponent<GceMetadataService> {
    /**
     * <p>Gets metadata on the current running machine (call to
     * http://metadata.google.internal/computeMetadata/v1/instance/xxx).</p>
     * <p>For example, you can retrieve network information by replacing xxx with:</p>
     * <ul>
     *     <li>`hostname` when we need to resolve the host name</li>
     *     <li>`network-interfaces/0/ip` when we need to resolve private IP</li>
     * </ul>
     * @see org.elasticsearch.cloud.gce.network.GceNameResolver for bindings
     * @param metadataPath path to metadata information
     * @return extracted information (for example a hostname or an IP address)
     * @throws IOException in case metadata URL is not accessible
     */
    String metadata(String metadataPath) throws IOException;
}
