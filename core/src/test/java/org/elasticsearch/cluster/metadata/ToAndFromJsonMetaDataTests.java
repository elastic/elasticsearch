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
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.AliasMetaData.newAliasMetaDataBuilder;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class ToAndFromJsonMetaDataTests extends ESTestCase {

    @Test
    public void testSimpleJsonFromAndTo() throws IOException {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(1)
                        .numberOfReplicas(2))
                .put(IndexMetaData.builder("test2")
                        .settings(settings(Version.CURRENT).put("setting1", "value1").put("setting2", "value2"))
                        .numberOfShards(2)
                        .numberOfReplicas(3))
                .put(IndexMetaData.builder("test3")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .putMapping("mapping1", MAPPING_SOURCE1))
                .put(IndexMetaData.builder("test4")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .creationDate(2l))
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
                        .creationDate(2l))
                .put(IndexMetaData.builder("test7")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .creationDate(2l)
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
                        .creationDate(2l)
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
                        .putAlias(newAliasMetaDataBuilder("alias2"))
                        .putAlias(newAliasMetaDataBuilder("alias4").filter(ALIAS_FILTER2)))
                        .put(IndexTemplateMetaData.builder("foo")
                                .template("bar")
                                .order(1)
                                .settings(settingsBuilder()
                                        .put("setting1", "value1")
                                        .put("setting2", "value2"))
                                .putAlias(newAliasMetaDataBuilder("alias-bar1"))
                                .putAlias(newAliasMetaDataBuilder("alias-bar2").filter("{\"term\":{\"user\":\"kimchy\"}}"))
                                .putAlias(newAliasMetaDataBuilder("alias-bar3").routing("routing-bar")))
                .put(IndexMetaData.builder("test12")
                        .settings(settings(Version.CURRENT)
                                .put("setting1", "value1")
                                .put("setting2", "value2"))
                        .creationDate(2l)
                        .numberOfShards(1)
                        .numberOfReplicas(2)
                        .putMapping("mapping1", MAPPING_SOURCE1)
                        .putMapping("mapping2", MAPPING_SOURCE2)
                        .putAlias(newAliasMetaDataBuilder("alias1").filter(ALIAS_FILTER1))
                        .putAlias(newAliasMetaDataBuilder("alias2"))
                        .putAlias(newAliasMetaDataBuilder("alias4").filter(ALIAS_FILTER2)))
                        .put(IndexTemplateMetaData.builder("foo")
                                .template("bar")
                                .order(1)
                                .settings(settingsBuilder()
                                        .put("setting1", "value1")
                                        .put("setting2", "value2"))
                                .putAlias(newAliasMetaDataBuilder("alias-bar1"))
                                .putAlias(newAliasMetaDataBuilder("alias-bar2").filter("{\"term\":{\"user\":\"kimchy\"}}"))
                                .putAlias(newAliasMetaDataBuilder("alias-bar3").routing("routing-bar")))
                .build();

        String metaDataSource = MetaData.Builder.toXContent(metaData);
//        System.out.println("ToJson: " + metaDataSource);

        MetaData parsedMetaData = MetaData.Builder.fromXContent(XContentFactory.xContent(XContentType.JSON).createParser(metaDataSource));

        IndexMetaData indexMetaData = parsedMetaData.index("test1");
        assertThat(indexMetaData.numberOfShards(), equalTo(1));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.creationDate(), equalTo(-1l));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(3));
        assertThat(indexMetaData.mappings().size(), equalTo(0));

        indexMetaData = parsedMetaData.index("test2");
        assertThat(indexMetaData.numberOfShards(), equalTo(2));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(3));
        assertThat(indexMetaData.creationDate(), equalTo(-1l));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(5));
        assertThat(indexMetaData.settings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.settings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.mappings().size(), equalTo(0));

        indexMetaData = parsedMetaData.index("test3");
        assertThat(indexMetaData.numberOfShards(), equalTo(1));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.creationDate(), equalTo(-1l));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(3));
        assertThat(indexMetaData.mappings().size(), equalTo(1));
        assertThat(indexMetaData.mappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));

        indexMetaData = parsedMetaData.index("test4");
        assertThat(indexMetaData.creationDate(), equalTo(2l));
        assertThat(indexMetaData.numberOfShards(), equalTo(1));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(4));
        assertThat(indexMetaData.mappings().size(), equalTo(0));

        indexMetaData = parsedMetaData.index("test5");
        assertThat(indexMetaData.numberOfShards(), equalTo(1));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.creationDate(), equalTo(-1l));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(5));
        assertThat(indexMetaData.settings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.settings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.mappings().size(), equalTo(2));
        assertThat(indexMetaData.mappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.mappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));

        indexMetaData = parsedMetaData.index("test6");
        assertThat(indexMetaData.numberOfShards(), equalTo(1));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.creationDate(), equalTo(2l));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(6));
        assertThat(indexMetaData.settings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.settings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.mappings().size(), equalTo(0));

        indexMetaData = parsedMetaData.index("test7");
        assertThat(indexMetaData.numberOfShards(), equalTo(1));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.creationDate(), equalTo(2l));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(4));
        assertThat(indexMetaData.mappings().size(), equalTo(2));
        assertThat(indexMetaData.mappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.mappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));

        indexMetaData = parsedMetaData.index("test8");
        assertThat(indexMetaData.numberOfShards(), equalTo(1));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.creationDate(), equalTo(-1l));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(5));
        assertThat(indexMetaData.settings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.settings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.mappings().size(), equalTo(2));
        assertThat(indexMetaData.mappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.mappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));
        assertThat(indexMetaData.aliases().size(), equalTo(2));
        assertThat(indexMetaData.aliases().get("alias1").alias(), equalTo("alias1"));
        assertThat(indexMetaData.aliases().get("alias2").alias(), equalTo("alias2"));

        indexMetaData = parsedMetaData.index("test9");
        assertThat(indexMetaData.numberOfShards(), equalTo(1));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.creationDate(), equalTo(2l));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(6));
        assertThat(indexMetaData.settings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.settings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.mappings().size(), equalTo(2));
        assertThat(indexMetaData.mappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.mappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));
        assertThat(indexMetaData.aliases().size(), equalTo(2));
        assertThat(indexMetaData.aliases().get("alias1").alias(), equalTo("alias1"));
        assertThat(indexMetaData.aliases().get("alias2").alias(), equalTo("alias2"));

        indexMetaData = parsedMetaData.index("test10");
        assertThat(indexMetaData.numberOfShards(), equalTo(1));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.creationDate(), equalTo(-1l));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(5));
        assertThat(indexMetaData.settings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.settings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.mappings().size(), equalTo(2));
        assertThat(indexMetaData.mappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.mappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));
        assertThat(indexMetaData.aliases().size(), equalTo(2));
        assertThat(indexMetaData.aliases().get("alias1").alias(), equalTo("alias1"));
        assertThat(indexMetaData.aliases().get("alias2").alias(), equalTo("alias2"));

        indexMetaData = parsedMetaData.index("test11");
        assertThat(indexMetaData.numberOfShards(), equalTo(1));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.creationDate(), equalTo(-1l));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(5));
        assertThat(indexMetaData.settings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.settings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.mappings().size(), equalTo(2));
        assertThat(indexMetaData.mappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.mappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));
        assertThat(indexMetaData.aliases().size(), equalTo(3));
        assertThat(indexMetaData.aliases().get("alias1").alias(), equalTo("alias1"));
        assertThat(indexMetaData.aliases().get("alias1").filter().string(), equalTo(ALIAS_FILTER1));
        assertThat(indexMetaData.aliases().get("alias2").alias(), equalTo("alias2"));
        assertThat(indexMetaData.aliases().get("alias2").filter(), nullValue());
        assertThat(indexMetaData.aliases().get("alias4").alias(), equalTo("alias4"));
        assertThat(indexMetaData.aliases().get("alias4").filter().string(), equalTo(ALIAS_FILTER2));

        indexMetaData = parsedMetaData.index("test12");
        assertThat(indexMetaData.numberOfShards(), equalTo(1));
        assertThat(indexMetaData.numberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.creationDate(), equalTo(2l));
        assertThat(indexMetaData.settings().getAsMap().size(), equalTo(6));
        assertThat(indexMetaData.settings().get("setting1"), equalTo("value1"));
        assertThat(indexMetaData.settings().get("setting2"), equalTo("value2"));
        assertThat(indexMetaData.mappings().size(), equalTo(2));
        assertThat(indexMetaData.mappings().get("mapping1").source().string(), equalTo(MAPPING_SOURCE1));
        assertThat(indexMetaData.mappings().get("mapping2").source().string(), equalTo(MAPPING_SOURCE2));
        assertThat(indexMetaData.aliases().size(), equalTo(3));
        assertThat(indexMetaData.aliases().get("alias1").alias(), equalTo("alias1"));
        assertThat(indexMetaData.aliases().get("alias1").filter().string(), equalTo(ALIAS_FILTER1));
        assertThat(indexMetaData.aliases().get("alias2").alias(), equalTo("alias2"));
        assertThat(indexMetaData.aliases().get("alias2").filter(), nullValue());
        assertThat(indexMetaData.aliases().get("alias4").alias(), equalTo("alias4"));
        assertThat(indexMetaData.aliases().get("alias4").filter().string(), equalTo(ALIAS_FILTER2));

        // templates
        assertThat(parsedMetaData.templates().get("foo").name(), is("foo"));
        assertThat(parsedMetaData.templates().get("foo").template(), is("bar"));
        assertThat(parsedMetaData.templates().get("foo").settings().get("index.setting1"), is("value1"));
        assertThat(parsedMetaData.templates().get("foo").settings().getByPrefix("index.").get("setting2"), is("value2"));
        assertThat(parsedMetaData.templates().get("foo").aliases().size(), equalTo(3));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar1").alias(), equalTo("alias-bar1"));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar2").alias(), equalTo("alias-bar2"));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar2").filter().string(), equalTo("{\"term\":{\"user\":\"kimchy\"}}"));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar3").alias(), equalTo("alias-bar3"));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar3").indexRouting(), equalTo("routing-bar"));
        assertThat(parsedMetaData.templates().get("foo").aliases().get("alias-bar3").searchRouting(), equalTo("routing-bar"));
    }

    private static final String MAPPING_SOURCE1 = "{\"mapping1\":{\"text1\":{\"type\":\"string\"}}}";
    private static final String MAPPING_SOURCE2 = "{\"mapping2\":{\"text2\":{\"type\":\"string\"}}}";
    private static final String ALIAS_FILTER1 = "{\"field1\":\"value1\"}";
    private static final String ALIAS_FILTER2 = "{\"field2\":\"value2\"}";
}
