/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logstashbridge;

import org.elasticsearch.script.Metadata;
import org.elasticsearch.test.ESTestCase;

import java.time.ZonedDateTime;

import static org.elasticsearch.logstashbridge.DucktypeMatchers.instancesQuackLike;

public class ScriptBridgeTests extends ESTestCase {

    public void testMetadataAPIStability() {
        interface MetadataInstanceAPI {
            ZonedDateTime getNow();

            String getIndex();

            void setIndex(String index);

            String getId();

            void setId(String id);

            long getVersion();

            void setVersion(long version);

            String getVersionType();

            void setVersionType(String versionType);

            String getRouting();

            void setRouting(String routing);
        }
        assertThat(Metadata.class, instancesQuackLike(MetadataInstanceAPI.class));
    }
}
