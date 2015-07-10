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

package org.elasticsearch.action.admin.cluster.node.info;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;

public class PluginsInfoTest extends ElasticsearchTestCase {

    @Test
    public void testPluginListSorted() {
        PluginsInfo pluginsInfo = new PluginsInfo(5);
        pluginsInfo.add(new PluginInfo("c", "foo", true, true, "dummy"));
        pluginsInfo.add(new PluginInfo("b", "foo", true, true, "dummy"));
        pluginsInfo.add(new PluginInfo("e", "foo", true, true, "dummy"));
        pluginsInfo.add(new PluginInfo("a", "foo", true, true, "dummy"));
        pluginsInfo.add(new PluginInfo("d", "foo", true, true, "dummy"));

        final List<PluginInfo> infos = pluginsInfo.getInfos();
        List<String> names = Lists.transform(infos, new Function<PluginInfo, String>() {
            @Override
            public String apply(PluginInfo input) {
                return input.getName();
            }
        });
        assertThat(names, contains("a", "b", "c", "d", "e"));
    }
}
