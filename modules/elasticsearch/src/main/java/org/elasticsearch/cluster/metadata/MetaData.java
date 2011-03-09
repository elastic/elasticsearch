/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this 
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.*;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.Immutable;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.collect.MapBuilder.*;
import static org.elasticsearch.common.collect.Sets.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;

/**
 * @author kimchy (shay.banon)
 */
@Immutable
public class MetaData implements Iterable<IndexMetaData> {

    public static final MetaData EMPTY_META_DATA = newMetaDataBuilder().build();

    private final ImmutableMap<String, IndexMetaData> indices;
    private final ImmutableMap<String, IndexTemplateMetaData> templates;

    private final transient int totalNumberOfShards;

    private final String[] allIndices;

    private final ImmutableSet<String> aliases;

    private final ImmutableMap<String, String[]> aliasAndIndexToIndexMap;
    private final ImmutableMap<String, ImmutableSet<String>> aliasAndIndexToIndexMap2;

    private MetaData(ImmutableMap<String, IndexMetaData> indices, ImmutableMap<String, IndexTemplateMetaData> templates) {
        this.indices = ImmutableMap.copyOf(indices);
        this.templates = templates;
        int totalNumberOfShards = 0;
        for (IndexMetaData indexMetaData : indices.values()) {
            totalNumberOfShards += indexMetaData.totalNumberOfShards();
        }
        this.totalNumberOfShards = totalNumberOfShards;

        // build all indices map
        List<String> allIndicesLst = Lists.newArrayList();
        for (IndexMetaData indexMetaData : indices.values()) {
            allIndicesLst.add(indexMetaData.index());
        }
        allIndices = allIndicesLst.toArray(new String[allIndicesLst.size()]);

        // build aliases set
        Set<String> aliases = newHashSet();
        for (IndexMetaData indexMetaData : indices.values()) {
            aliases.addAll(indexMetaData.aliases());
        }
        this.aliases = ImmutableSet.copyOf(aliases);

        // build aliasAndIndex to Index map
        MapBuilder<String, Set<String>> tmpAliasAndIndexToIndexBuilder = newMapBuilder();
        for (IndexMetaData indexMetaData : indices.values()) {
            Set<String> lst = tmpAliasAndIndexToIndexBuilder.get(indexMetaData.index());
            if (lst == null) {
                lst = newHashSet();
                tmpAliasAndIndexToIndexBuilder.put(indexMetaData.index(), lst);
            }
            lst.add(indexMetaData.index());

            for (String alias : indexMetaData.aliases()) {
                lst = tmpAliasAndIndexToIndexBuilder.get(alias);
                if (lst == null) {
                    lst = newHashSet();
                    tmpAliasAndIndexToIndexBuilder.put(alias, lst);
                }
                lst.add(indexMetaData.index());
            }
        }

        MapBuilder<String, String[]> aliasAndIndexToIndexBuilder = newMapBuilder();
        for (Map.Entry<String, Set<String>> entry : tmpAliasAndIndexToIndexBuilder.map().entrySet()) {
            aliasAndIndexToIndexBuilder.put(entry.getKey(), entry.getValue().toArray(new String[entry.getValue().size()]));
        }
        this.aliasAndIndexToIndexMap = aliasAndIndexToIndexBuilder.immutableMap();

        MapBuilder<String, ImmutableSet<String>> aliasAndIndexToIndexBuilder2 = newMapBuilder();
        for (Map.Entry<String, Set<String>> entry : tmpAliasAndIndexToIndexBuilder.map().entrySet()) {
            aliasAndIndexToIndexBuilder2.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
        }
        this.aliasAndIndexToIndexMap2 = aliasAndIndexToIndexBuilder2.immutableMap();
    }

    public ImmutableSet<String> aliases() {
        return this.aliases;
    }

    public ImmutableSet<String> getAliases() {
        return aliases();
    }

    /**
     * Returns all the concrete indices.
     */
    public String[] concreteAllIndices() {
        return allIndices;
    }

    public String[] getConcreteAllIndices() {
        return concreteAllIndices();
    }

    /**
     * Translates the provided indices (possibly aliased) into actual indices.
     */
    public String[] concreteIndices(String[] indices) throws IndexMissingException {
        return concreteIndices(indices, false);
    }

    /**
     * Translates the provided indices (possibly aliased) into actual indices.
     */
    public String[] concreteIndicesIgnoreMissing(String[] indices) {
        return concreteIndices(indices, true);
    }

    /**
     * Translates the provided indices (possibly aliased) into actual indices.
     */
    public String[] concreteIndices(String[] indices, boolean ignoreMissing) throws IndexMissingException {
        if (indices == null || indices.length == 0) {
            return concreteAllIndices();
        }
        // optimize for single element index (common case)
        if (indices.length == 1) {
            String index = indices[0];
            if (index.length() == 0) {
                return concreteAllIndices();
            }
            if (index.equals("_all")) {
                return concreteAllIndices();
            }
            // if a direct index name, just return the array provided
            if (this.indices.containsKey(index)) {
                return indices;
            }
            String[] actualLst = aliasAndIndexToIndexMap.get(index);
            if (actualLst == null) {
                if (!ignoreMissing) {
                    throw new IndexMissingException(new Index(index));
                } else {
                    return Strings.EMPTY_ARRAY;
                }
            } else {
                return actualLst;
            }
        }

        // check if its a possible aliased index, if not, just return the
        // passed array
        boolean possiblyAliased = false;
        for (String index : indices) {
            if (!this.indices.containsKey(index)) {
                possiblyAliased = true;
            }
        }
        if (!possiblyAliased) {
            return indices;
        }

        ArrayList<String> actualIndices = Lists.newArrayListWithCapacity(indices.length);
        for (String index : indices) {
            String[] actualLst = aliasAndIndexToIndexMap.get(index);
            if (actualLst == null) {
                if (!ignoreMissing) {
                    throw new IndexMissingException(new Index(index));
                }
            } else {
                for (String x : actualLst) {
                    actualIndices.add(x);
                }
            }
        }
        return actualIndices.toArray(new String[actualIndices.size()]);
    }

    public String concreteIndex(String index) throws IndexMissingException, ElasticSearchIllegalArgumentException {
        // a quick check, if this is an actual index, if so, return it
        if (indices.containsKey(index)) {
            return index;
        }
        // not an actual index, fetch from an alias
        String[] lst = aliasAndIndexToIndexMap.get(index);
        if (lst == null) {
            throw new IndexMissingException(new Index(index));
        }
        if (lst.length > 1) {
            throw new ElasticSearchIllegalArgumentException("Alias [" + index + "] has more than one indices associated with it [" + Arrays.toString(lst) + "], can't execute a single index op");
        }
        return lst[0];
    }

    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    public boolean hasConcreteIndex(String index) {
        return aliasAndIndexToIndexMap2.containsKey(index);
    }

    public IndexMetaData index(String index) {
        return indices.get(index);
    }

    public ImmutableMap<String, IndexMetaData> indices() {
        return this.indices;
    }

    public ImmutableMap<String, IndexMetaData> getIndices() {
        return indices();
    }

    public ImmutableMap<String, IndexTemplateMetaData> templates() {
        return this.templates;
    }

    public ImmutableMap<String, IndexTemplateMetaData> getTemplates() {
        return this.templates;
    }

    public int totalNumberOfShards() {
        return this.totalNumberOfShards;
    }

    public int getTotalNumberOfShards() {
        return totalNumberOfShards();
    }

    @Override public UnmodifiableIterator<IndexMetaData> iterator() {
        return indices.values().iterator();
    }

    public static Builder builder() {
        return new Builder();
    }


    public static Builder newMetaDataBuilder() {
        return new Builder();
    }

    public static class Builder {

        private MapBuilder<String, IndexMetaData> indices = newMapBuilder();

        private MapBuilder<String, IndexTemplateMetaData> templates = newMapBuilder();

        public Builder metaData(MetaData metaData) {
            this.indices.putAll(metaData.indices);
            this.templates.putAll(metaData.templates);
            return this;
        }

        public Builder put(IndexMetaData.Builder indexMetaDataBuilder) {
            return put(indexMetaDataBuilder.build());
        }

        public Builder put(IndexMetaData indexMetaData) {
            indices.put(indexMetaData.index(), indexMetaData);
            return this;
        }

        public IndexMetaData get(String index) {
            return indices.get(index);
        }

        public Builder remove(String index) {
            indices.remove(index);
            return this;
        }

        public Builder put(IndexTemplateMetaData.Builder template) {
            return put(template.build());
        }

        public Builder put(IndexTemplateMetaData template) {
            templates.put(template.name(), template);
            return this;
        }

        public Builder remoteTemplate(String templateName) {
            templates.remove(templateName);
            return this;
        }

        public Builder updateSettings(Settings settings, String... indices) {
            if (indices == null || indices.length == 0) {
                indices = this.indices.map().keySet().toArray(new String[this.indices.map().keySet().size()]);
            }
            for (String index : indices) {
                IndexMetaData indexMetaData = this.indices.get(index);
                if (indexMetaData == null) {
                    throw new IndexMissingException(new Index(index));
                }
                put(IndexMetaData.newIndexMetaDataBuilder(indexMetaData)
                        .settings(settingsBuilder().put(indexMetaData.settings()).put(settings))
                        .build());
            }
            return this;
        }

        public Builder updateNumberOfReplicas(int numberOfReplicas, String... indices) {
            if (indices == null || indices.length == 0) {
                indices = this.indices.map().keySet().toArray(new String[this.indices.map().keySet().size()]);
            }
            for (String index : indices) {
                IndexMetaData indexMetaData = this.indices.get(index);
                if (indexMetaData == null) {
                    throw new IndexMissingException(new Index(index));
                }
                put(IndexMetaData.newIndexMetaDataBuilder(indexMetaData).numberOfReplicas(numberOfReplicas).build());
            }
            return this;
        }

        public MetaData build() {
            return new MetaData(indices.immutableMap(), templates.immutableMap());
        }

        public static String toXContent(MetaData metaData) throws IOException {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            toXContent(metaData, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        }

        public static void toXContent(MetaData metaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject("meta-data");

            builder.startObject("templates");
            for (IndexTemplateMetaData template : metaData.templates().values()) {
                IndexTemplateMetaData.Builder.toXContent(template, builder, params);
            }
            builder.endObject();

            builder.startObject("indices");
            for (IndexMetaData indexMetaData : metaData) {
                IndexMetaData.Builder.toXContent(indexMetaData, builder, params);
            }
            builder.endObject();

            builder.endObject();
        }

        public static MetaData fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder();

            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (!"meta-data".equals(currentFieldName)) {
                token = parser.nextToken();
                currentFieldName = parser.currentName();
                if (token == null) {
                    // no data...
                    return builder.build();
                }
            }

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("indices".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexMetaData.Builder.fromXContent(parser));
                        }
                    } else if ("templates".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexTemplateMetaData.Builder.fromXContent(parser));
                        }
                    }
                }
            }
            return builder.build();
        }

        public static MetaData readFrom(StreamInput in) throws IOException {
            Builder builder = new Builder();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                builder.put(IndexMetaData.Builder.readFrom(in));
            }
            size = in.readVInt();
            for (int i = 0; i < size; i++) {
                builder.put(IndexTemplateMetaData.Builder.readFrom(in));
            }
            return builder.build();
        }

        public static void writeTo(MetaData metaData, StreamOutput out) throws IOException {
            out.writeVInt(metaData.indices.size());
            for (IndexMetaData indexMetaData : metaData) {
                IndexMetaData.Builder.writeTo(indexMetaData, out);
            }
            out.writeVInt(metaData.templates.size());
            for (IndexTemplateMetaData template : metaData.templates.values()) {
                IndexTemplateMetaData.Builder.writeTo(template, out);
            }
        }
    }
}
