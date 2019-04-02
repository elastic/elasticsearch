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

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.cluster.metadata.AliasMetaData.newAliasMetaDataBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ToAndFromJsonMetaDataTests extends ESTestCase {

    public void testSimpleJsonFromAndTo() throws IOException {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .primaryTerm(0, 1))
                .put(IndexMetaData.builder("test2")
                        .settings(settings(Version.CURRENT).put("setting1", "value1").put("setting2", "value2"))
                        .numberOfShards(2)
                        .numberOfReplicas(3)
                        .primaryTerm(0, 2)
                        .primaryTerm(1, 2))
                .put(IndexMetaData.builder("test3")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .putMapping("mapping1", MAPPING_SOURCE1))
                .put(IndexMetaData.builder("test4")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .creationDate(2L))
                .put(IndexMetaData.builder("test5")
                        .settings(settings(Version.CURRENT).put("setting1", "value1").put("setting2", "value2"))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .putMapping("mapping1", MAPPING_SOURCE1)
                        .putMapping("mapping2", MAPPING_SOURCE2))
                .put(IndexMetaData.builder("test6")
                        .settings(settings(Version.CURRENT).put("setting1", "value1").put("setting2", "value2"))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .creationDate(2L))
                .put(IndexMetaData.builder("test7")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .creationDate(2L)
                        .putMapping("mapping1", MAPPING_SOURCE1)
                        .putMapping("mapping2", MAPPING_SOURCE2))
                .put(IndexMetaData.builder("test8")
                        .settings(settings(Version.CURRENT).put("setting1", "value1").put("setting2", "value2"))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .putMapping("mapping1", MAPPING_SOURCE1)
                        .putMapping("mapping2", MAPPING_SOURCE2)
                        .putAlias(newAliasMetaDataBuilder("alias1"))
                        .putAlias(newAliasMetaDataBuilder("alias2")))
                .put(IndexMetaData.builder("test9")
                        .settings(settings(Version.CURRENT).put("setting1", "value1").put("setting2", "value2"))
                        .creationDate(2L)
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .putMapping("mapping1", MAPPING_SOURCE1)
                        .putMapping("mapping2", MAPPING_SOURCE2)
                        .putAlias(newAliasMetaDataBuilder("alias1"))
                        .putAlias(newAliasMetaDataBuilder("alias2")))
                .put(IndexMetaData.builder("test10")
                        .settings(settings(Version.CURRENT)
                                .put("setting1", "value1")
                                .put("setting2", "value2"))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .putMapping("mapping1", MAPPING_SOURCE1)
                        .putMapping("mapping2", MAPPING_SOURCE2)
                        .putAlias(newAliasMetaDataBuilder("alias1"))
                        .putAlias(newAliasMetaDataBuilder("alias2")))
                .put(IndexMetaData.builder("test11")
                        .settings(settings(Version.CURRENT)
                                .put("setting1", "value1")
                                .put("setting2", "value2"))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .putMapping("mapping1", MAPPING_SOURCE1)
                        .putMapping("mapping2", MAPPING_SOURCE2)
                        .putAlias(newAliasMetaDataBuilder("alias1").filter(ALIAS_FILTER1))
                        .putAlias(newAliasMetaDataBuilder("alias2").writeIndex(randomBoolean() ? null : randomBoolean()))
                        .putAlias(newAliasMetaDataBuilder("alias4").filter(ALIAS_FILTER2)))
                .put(IndexTemplateMetaData.builder("foo")
                        .patterns(Collections.singletonList("bar"))
                        .order(1)
                        .settings(Settings.builder()
                                .put("setting1", "value1")
                                .put("setting2", "value2"))
                        .putAlias(newAliasMetaDataBuilder("alias-bar1"))
                        .putAlias(newAliasMetaDataBuilder("alias-bar2").filter("{\"term\":{\"user\":\"kimchy\"}}"))
                        .putAlias(newAliasMetaDataBuilder("alias-bar3").routing("routing-bar")))
                .put(IndexMetaData.builder("test12")
                        .settings(settings(Version.CURRENT)
                                .put("setting1", "value1")
                                .put("setting2", "value2"))
                        .creationDate(2L)
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .putMapping("mapping1", MAPPING_SOURCE1)
                        .putMapping("mapping2", MAPPING_SOURCE2)
                        .putAlias(newAliasMetaDataBuilder("alias1").filter(ALIAS_FILTER1))
                        .putAlias(newAliasMetaDataBuilder("alias3").writeIndex(randomBoolean() ? null : randomBoolean()))
                        .putAlias(newAliasMetaDataBuilder("alias4").filter(ALIAS_FILTER2)))
                .put(IndexTemplateMetaData.builder("foo")
                        .patterns(Collections.singletonList("bar"))
                        .order(1)
                        .settings(Settings.builder()
                                .put("setting1", "value1")
                                .put("setting2", "value2"))
                        .putAlias(newAliasMetaDataBuilder("alias-bar1"))
                        .putAlias(newAliasMetaDataBuilder("alias-bar2").filter("{\"term\":{\"user\":\"kimchy\"}}"))
                        .putAlias(newAliasMetaDataBuilder("alias-bar3").routing("routing-bar")))
                .build();

        String metaDataSource = MetaData.Builder.toXContent(metaData);

        MetaData parsedMetaData = MetaData.Builder.fromXContent(createParser(JsonXContent.jsonXContent, metaDataSource));

        IndexMetaData indexMetaData = parsedMetaData.index("test1");
        assertThat(indexMetaData.primaryTerm(0), equalTo(1L));
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getCreationDate(), equalTo(-1L));
        assertThat(indexMetaData.getSettings().size(), equalTo(3));
        assertThat(indexMetaData.getMappings().size(), equalTo(0));

        indexMetaData = parsedMetaData.index("test2");
        assertThat(indexMetaData.getNumberOfShards(), equalTo(2));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(3));
        assertThat(indexMetaData.primaryTerm(0), equalTo(2L));
        assertThat(indexMetaData.primaryTerm(1), equalTo(2L));
        assertThat(indexMetaData.getCreationDate(), equalTo(-1L));
        assertThat(indexMetaData.getSettings().size(), equalTo(5));
        assertThat(indexMetaData.getSettings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.getSettings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.getMappings().size(), equalTo(0));

        indexMetaData = parsedMetaData.index("test3");
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getCreationDate(), equalTo(-1L));
        assertThat(indexMetaData.getSettings().size(), equalTo(3));
        assertThat(indexMetaData.getMappings().size(), equalTo(1));
        assertThat(indexMetaData.getMappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));

        indexMetaData = parsedMetaData.index("test4");
        assertThat(indexMetaData.getCreationDate(), equalTo(2L));
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getSettings().size(), equalTo(4));
        assertThat(indexMetaData.getMappings().size(), equalTo(0));

        indexMetaData = parsedMetaData.index("test5");
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getCreationDate(), equalTo(-1L));
        assertThat(indexMetaData.getSettings().size(), equalTo(5));
        assertThat(indexMetaData.getSettings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.getSettings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.getMappings().size(), equalTo(2));
        assertThat(indexMetaData.getMappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.getMappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));

        indexMetaData = parsedMetaData.index("test6");
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getCreationDate(), equalTo(2L));
        assertThat(indexMetaData.getSettings().size(), equalTo(6));
        assertThat(indexMetaData.getSettings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.getSettings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.getMappings().size(), equalTo(0));

        indexMetaData = parsedMetaData.index("test7");
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getCreationDate(), equalTo(2L));
        assertThat(indexMetaData.getSettings().size(), equalTo(4));
        assertThat(indexMetaData.getMappings().size(), equalTo(2));
        assertThat(indexMetaData.getMappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.getMappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));

        indexMetaData = parsedMetaData.index("test8");
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getCreationDate(), equalTo(-1L));
        assertThat(indexMetaData.getSettings().size(), equalTo(5));
        assertThat(indexMetaData.getSettings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.getSettings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.getMappings().size(), equalTo(2));
        assertThat(indexMetaData.getMappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.getMappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));
        assertThat(indexMetaData.getAliases().size(), equalTo(2));
        assertThat(indexMetaData.getAliases().get("alias1").alias(), equalTo("alias1"));
        assertThat(indexMetaData.getAliases().get("alias2").alias(), equalTo("alias2"));

        indexMetaData = parsedMetaData.index("test9");
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getCreationDate(), equalTo(2L));
        assertThat(indexMetaData.getSettings().size(), equalTo(6));
        assertThat(indexMetaData.getSettings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.getSettings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.getMappings().size(), equalTo(2));
        assertThat(indexMetaData.getMappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.getMappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));
        assertThat(indexMetaData.getAliases().size(), equalTo(2));
        assertThat(indexMetaData.getAliases().get("alias1").alias(), equalTo("alias1"));
        assertThat(indexMetaData.getAliases().get("alias2").alias(), equalTo("alias2"));

        indexMetaData = parsedMetaData.index("test10");
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getCreationDate(), equalTo(-1L));
        assertThat(indexMetaData.getSettings().size(), equalTo(5));
        assertThat(indexMetaData.getSettings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.getSettings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.getMappings().size(), equalTo(2));
        assertThat(indexMetaData.getMappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.getMappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));
        assertThat(indexMetaData.getAliases().size(), equalTo(2));
        assertThat(indexMetaData.getAliases().get("alias1").alias(), equalTo("alias1"));
        assertThat(indexMetaData.getAliases().get("alias2").alias(), equalTo("alias2"));

        indexMetaData = parsedMetaData.index("test11");
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getCreationDate(), equalTo(-1L));
        assertThat(indexMetaData.getSettings().size(), equalTo(5));
        assertThat(indexMetaData.getSettings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.getSettings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.getMappings().size(), equalTo(2));
        assertThat(indexMetaData.getMappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.getMappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));
        assertThat(indexMetaData.getAliases().size(), equalTo(3));
        assertThat(indexMetaData.getAliases().get("alias1").alias(), equalTo("alias1"));
        assertThat(indexMetaData.getAliases().get("alias1").filter().string(), equalTo(ALIAS_FILTER1));
        assertThat(indexMetaData.getAliases().get("alias2").alias(), equalTo("alias2"));
        assertThat(indexMetaData.getAliases().get("alias2").filter(), nullValue());
        assertThat(indexMetaData.getAliases().get("alias2").writeIndex(),
            equalTo(metaData.index("test11").getAliases().get("alias2").writeIndex()));
        assertThat(indexMetaData.getAliases().get("alias4").alias(), equalTo("alias4"));
        assertThat(indexMetaData.getAliases().get("alias4").filter().string(), equalTo(ALIAS_FILTER2));

        indexMetaData = parsedMetaData.index("test12");
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getCreationDate(), equalTo(2L));
        assertThat(indexMetaData.getSettings().size(), equalTo(6));
        assertThat(indexMetaData.getSettings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.getSettings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.getMappings().size(), equalTo(2));
        assertThat(indexMetaData.getMappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.getMappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));
        assertThat(indexMetaData.getAliases().size(), equalTo(3));
        assertThat(indexMetaData.getAliases().get("alias1").alias(), equalTo("alias1"));
        assertThat(indexMetaData.getAliases().get("alias1").filter().string(), equalTo(ALIAS_FILTER1));
        assertThat(indexMetaData.getAliases().get("alias3").alias(), equalTo("alias3"));
        assertThat(indexMetaData.getAliases().get("alias3").filter(), nullValue());
        assertThat(indexMetaData.getAliases().get("alias3").writeIndex(),
            equalTo(metaData.index("test12").getAliases().get("alias3").writeIndex()));
        assertThat(indexMetaData.getAliases().get("alias4").alias(), equalTo("alias4"));
        assertThat(indexMetaData.getAliases().get("alias4").filter().string(), equalTo(ALIAS_FILTER2));

        // templates
        assertThat(parsedMetaData.templates().get("foo").name(), is("foo"));
        assertThat(parsedMetaData.templates().get("foo").patterns(), is(Collections.singletonList("bar")));
        assertThat(parsedMetaData.templates().get("foo").settings().get("index.setting1"), is("value1"));
        assertThat(parsedMetaData.templates().get("foo").settings().getByPrefix("index.").get("setting2"), is("value2"));
        assertThat(parsedMetaData.templates().get("foo").aliases().size(), equalTo(3));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar1").alias(), equalTo("alias-bar1"));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar2").alias(), equalTo("alias-bar2"));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar2").filter().string(),
            equalTo("{\"term\":{\"user\":\"kimchy\"}}"));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar3").alias(), equalTo("alias-bar3"));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar3").indexRouting(), equalTo("routing-bar"));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar3").searchRouting(), equalTo("routing-bar"));
    }

    private static final String MAPPING_SOURCE1 = "{\"mapping1\":{\"text1\":{\"type\":\"string\"}}}";
    private static final String MAPPING_SOURCE2 = "{\"mapping2\":{\"text2\":{\"type\":\"string\"}}}";
    private static final String ALIAS_FILTER1 = "{\"field1\":\"value1\"}";
    private static final String ALIAS_FILTER2 = "{\"field2\":\"value2\"}";
}
