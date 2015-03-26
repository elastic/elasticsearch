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

package org.elasticsearch.gateway;

import com.google.common.collect.Maps;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Handles writing and loading both {@link MetaData} and {@link IndexMetaData}
 */
public class MetaStateService extends AbstractComponent {

    static final String FORMAT_SETTING = "gateway.format";

    static final String GLOBAL_STATE_FILE_PREFIX = "global-";
    private static final String INDEX_STATE_FILE_PREFIX = "state-";
    static final Pattern GLOBAL_STATE_FILE_PATTERN = Pattern.compile(GLOBAL_STATE_FILE_PREFIX + "(\\d+)(" + MetaDataStateFormat.STATE_FILE_EXTENSION + ")?");
    static final Pattern INDEX_STATE_FILE_PATTERN = Pattern.compile(INDEX_STATE_FILE_PREFIX + "(\\d+)(" + MetaDataStateFormat.STATE_FILE_EXTENSION + ")?");
    private static final String GLOBAL_STATE_LOG_TYPE = "[_global]";

    private final NodeEnvironment nodeEnv;

    private final XContentType format;
    private final ToXContent.Params formatParams;
    private final ToXContent.Params gatewayModeFormatParams;

    @Inject
    public MetaStateService(Settings settings, NodeEnvironment nodeEnv) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.format = XContentType.fromRestContentType(settings.get(FORMAT_SETTING, "smile"));

        if (this.format == XContentType.SMILE) {
            Map<String, String> params = Maps.newHashMap();
            params.put("binary", "true");
            formatParams = new ToXContent.MapParams(params);
            Map<String, String> gatewayModeParams = Maps.newHashMap();
            gatewayModeParams.put("binary", "true");
            gatewayModeParams.put(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_GATEWAY);
            gatewayModeFormatParams = new ToXContent.MapParams(gatewayModeParams);
        } else {
            formatParams = ToXContent.EMPTY_PARAMS;
            Map<String, String> gatewayModeParams = Maps.newHashMap();
            gatewayModeParams.put(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_GATEWAY);
            gatewayModeFormatParams = new ToXContent.MapParams(gatewayModeParams);
        }
    }

    /**
     * Loads the full state, which includes both the global state and all the indices
     * meta state.
     */
    MetaData loadFullState() throws Exception {
        MetaData globalMetaData = loadGlobalState();
        MetaData.Builder metaDataBuilder;
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
        } else {
            metaDataBuilder = MetaData.builder();
        }

        final Set<String> indices = nodeEnv.findAllIndices();
        for (String index : indices) {
            IndexMetaData indexMetaData = loadIndexState(index);
            if (indexMetaData == null) {
                logger.debug("[{}] failed to find metadata for existing index location", index);
            } else {
                metaDataBuilder.put(indexMetaData, false);
            }
        }
        return metaDataBuilder.build();
    }

    /**
     * Loads the index state for the provided index name, returning null if doesn't exists.
     */
    @Nullable
    IndexMetaData loadIndexState(String index) throws IOException {
        return MetaDataStateFormat.loadLatestState(logger, indexStateFormat(format, formatParams, true),
                INDEX_STATE_FILE_PATTERN, "[" + index + "]", nodeEnv.indexPaths(new Index(index)));
    }

    /**
     * Loads the global state, *without* index state, see {@link #loadFullState()} for that.
     */
    MetaData loadGlobalState() throws IOException {
        return MetaDataStateFormat.loadLatestState(logger, globalStateFormat(format, gatewayModeFormatParams, true), GLOBAL_STATE_FILE_PATTERN, GLOBAL_STATE_LOG_TYPE, nodeEnv.nodeDataPaths());
    }

    /**
     * Writes the index state.
     */
    void writeIndex(String reason, IndexMetaData indexMetaData, @Nullable IndexMetaData previousIndexMetaData) throws Exception {
        logger.trace("[{}] writing state, reason [{}]", indexMetaData.index(), reason);
        final boolean deleteOldFiles = previousIndexMetaData != null && previousIndexMetaData.version() != indexMetaData.version();
        final MetaDataStateFormat<IndexMetaData> writer = indexStateFormat(format, formatParams, deleteOldFiles);
        try {
            writer.write(indexMetaData, INDEX_STATE_FILE_PREFIX, indexMetaData.version(),
                    nodeEnv.indexPaths(new Index(indexMetaData.index())));
        } catch (Throwable ex) {
            logger.warn("[{}]: failed to write index state", ex, indexMetaData.index());
            throw new IOException("failed to write state for [" + indexMetaData.index() + "]", ex);
        }
    }

    /**
     * Writes the global state, *without* the indices states.
     */
    void writeGlobalState(String reason, MetaData metaData) throws Exception {
        logger.trace("{} writing state, reason [{}]", GLOBAL_STATE_LOG_TYPE, reason);
        final MetaDataStateFormat<MetaData> writer = globalStateFormat(format, gatewayModeFormatParams, true);
        try {
            writer.write(metaData, GLOBAL_STATE_FILE_PREFIX, metaData.version(), nodeEnv.nodeDataPaths());
        } catch (Throwable ex) {
            logger.warn("{}: failed to write global state", ex, GLOBAL_STATE_LOG_TYPE);
            throw new IOException("failed to write global state", ex);
        }
    }

    /**
     * Returns a StateFormat that can read and write {@link MetaData}
     */
    static MetaDataStateFormat<MetaData> globalStateFormat(XContentType format, final ToXContent.Params formatParams, final boolean deleteOldFiles) {
        return new MetaDataStateFormat<MetaData>(format, deleteOldFiles) {

            @Override
            public void toXContent(XContentBuilder builder, MetaData state) throws IOException {
                MetaData.Builder.toXContent(state, builder, formatParams);
            }

            @Override
            public MetaData fromXContent(XContentParser parser) throws IOException {
                return MetaData.Builder.fromXContent(parser);
            }
        };
    }

    /**
     * Returns a StateFormat that can read and write {@link IndexMetaData}
     */
    static MetaDataStateFormat<IndexMetaData> indexStateFormat(XContentType format, final ToXContent.Params formatParams, boolean deleteOldFiles) {
        return new MetaDataStateFormat<IndexMetaData>(format, deleteOldFiles) {

            @Override
            public void toXContent(XContentBuilder builder, IndexMetaData state) throws IOException {
                IndexMetaData.Builder.toXContent(state, builder, formatParams);            }

            @Override
            public IndexMetaData fromXContent(XContentParser parser) throws IOException {
                return IndexMetaData.Builder.fromXContent(parser);
            }
        };
    }
}
