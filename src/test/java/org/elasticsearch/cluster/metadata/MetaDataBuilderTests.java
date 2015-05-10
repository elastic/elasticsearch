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
package org.elasticsearch.cluster.metadata;

import com.sun.xml.internal.ws.api.config.management.policy.ManagementAssertion;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;
import org.junit.Assert.*;

/**
 * Created by comdiv on 10.05.2015.
 */
public class MetaDataBuilderTests extends ElasticsearchTestCase {
    @Test
    public void CanUpdateSettings(){
        ImmutableSettings.Builder targetBuilder = ImmutableSettings.settingsBuilder();
        targetBuilder.put("a","1");
        targetBuilder.put("b","2");
        Settings target = targetBuilder.build();
        ImmutableSettings.Builder sourceBuilder = ImmutableSettings.settingsBuilder();
        sourceBuilder.put("a","3");
        sourceBuilder.put("c","4");
        Settings source = sourceBuilder.build();
        Settings merged = MetaData.Builder.getUpdatedSettings(source,target);
        assertNotSame("it's new instance",merged,source);
        assertNotSame("it's new instance",merged,target);
        assertEquals("3", merged.get("a"));
        assertEquals("2", merged.get("b"));
        assertEquals("4",merged.get("c"));
    }

    @Test
    public void SupportSettingRemoval(){
        ImmutableSettings.Builder targetBuilder = ImmutableSettings.settingsBuilder();
        targetBuilder.put("a","1");
        targetBuilder.put("b","2");
        Settings target = targetBuilder.build();
        ImmutableSettings.Builder sourceBuilder = ImmutableSettings.settingsBuilder();
        sourceBuilder.put("a","__REMOVE__");
        sourceBuilder.put("c","4");
        Settings source = sourceBuilder.build();
        Settings merged = MetaData.Builder.getUpdatedSettings(source,target);
        assertFalse(merged.getAsStructuredMap().containsKey("a"));
        assertEquals("2",merged.get("b"));
        assertEquals("4",merged.get("c"));
    }

    @Test
    public void SupportHierarhicalSettingRemoval(){
        ImmutableSettings.Builder targetBuilder = ImmutableSettings.settingsBuilder();
        targetBuilder.put("a.x","1");
        targetBuilder.put("a.y","2");
        targetBuilder.put("b","2");
        Settings target = targetBuilder.build();
        ImmutableSettings.Builder sourceBuilder = ImmutableSettings.settingsBuilder();
        sourceBuilder.put("a","__REMOVE__");
        sourceBuilder.put("c","4");
        Settings source = sourceBuilder.build();
        Settings merged = MetaData.Builder.getUpdatedSettings(source,target);
        assertFalse(merged.getAsStructuredMap().containsKey("a"));
        assertFalse(merged.getAsStructuredMap().containsKey("a.x"));
        assertFalse(merged.getAsStructuredMap().containsKey("a.y"));
        assertEquals("2",merged.get("b"));
        assertEquals("4",merged.get("c"));
    }
}
