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

package org.elasticsearch.action.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestDocument.MetaData;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SimulatePipelineRequest extends ActionRequest implements ToXContentObject {

    private static final Logger logger = LogManager.getLogger(SimulatePipelineRequest.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    private String id;
    private boolean verbose;
    private BytesReference source;
    private XContentType xContentType;

    /**
     * Creates a new request with the given source and its content type
     */
    public SimulatePipelineRequest(BytesReference source, XContentType xContentType) {
        this.source = Objects.requireNonNull(source);
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    SimulatePipelineRequest() {
    }

    SimulatePipelineRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readOptionalString();
        verbose = in.readBoolean();
        source = in.readBytesReference();
        xContentType = in.readEnum(XContentType.class);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public BytesReference getSource() {
        return source;
    }

    public XContentType getXContentType() {
        return xContentType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        out.writeBoolean(verbose);
        out.writeBytesReference(source);
        out.writeEnum(xContentType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.rawValue(source.streamInput(), xContentType);
        return builder;
    }

    public static final class Fields {
        static final String PIPELINE = "pipeline";
        static final String DOCS = "docs";
        static final String SOURCE = "_source";
    }

    static class Parsed {
        private final List<IngestDocument> documents;
        private final Pipeline pipeline;
        private final boolean verbose;

        Parsed(Pipeline pipeline, List<IngestDocument> documents, boolean verbose) {
            this.pipeline = pipeline;
            this.documents = Collections.unmodifiableList(documents);
            this.verbose = verbose;
        }

        public Pipeline getPipeline() {
            return pipeline;
        }

        public List<IngestDocument> getDocuments() {
            return documents;
        }

        public boolean isVerbose() {
            return verbose;
        }
    }

    static final String SIMULATED_PIPELINE_ID = "_simulate_pipeline";

    static Parsed parseWithPipelineId(String pipelineId, Map<String, Object> config, boolean verbose, IngestService ingestService) {
        if (pipelineId == null) {
            throw new IllegalArgumentException("param [pipeline] is null");
        }
        Pipeline pipeline = ingestService.getPipeline(pipelineId);
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline [" + pipelineId + "] does not exist");
        }
        List<IngestDocument> ingestDocumentList = parseDocs(config);
        return new Parsed(pipeline, ingestDocumentList, verbose);
    }

    static Parsed parse(Map<String, Object> config, boolean verbose, IngestService ingestService) throws Exception {
        Map<String, Object> pipelineConfig = ConfigurationUtils.readMap(null, null, config, Fields.PIPELINE);
        Pipeline pipeline = Pipeline.create(
            SIMULATED_PIPELINE_ID, pipelineConfig, ingestService.getProcessorFactories(), ingestService.getScriptService()
        );
        List<IngestDocument> ingestDocumentList = parseDocs(config);
        return new Parsed(pipeline, ingestDocumentList, verbose);
    }

    private static List<IngestDocument> parseDocs(Map<String, Object> config) {
        List<Map<String, Object>> docs =
            ConfigurationUtils.readList(null, null, config, Fields.DOCS);
        List<IngestDocument> ingestDocumentList = new ArrayList<>();
        for (Map<String, Object> dataMap : docs) {
            Map<String, Object> document = ConfigurationUtils.readMap(null, null,
                dataMap, Fields.SOURCE);
            String index = ConfigurationUtils.readStringOrIntProperty(null, null,
                dataMap, MetaData.INDEX.getFieldName(), "_index");
            if (dataMap.containsKey(MetaData.TYPE.getFieldName())) {
                deprecationLogger.deprecatedAndMaybeLog("simulate_pipeline_with_types",
                    "[types removal] specifying _type in pipeline simulation requests is deprecated");
            }
            String type = ConfigurationUtils.readStringOrIntProperty(null, null,
                dataMap, MetaData.TYPE.getFieldName(), "_doc");
            String id = ConfigurationUtils.readStringOrIntProperty(null, null,
                dataMap, MetaData.ID.getFieldName(), "_id");
            String routing = ConfigurationUtils.readOptionalStringOrIntProperty(null, null,
                dataMap, MetaData.ROUTING.getFieldName());
            Long version = null;
            if (dataMap.containsKey(MetaData.VERSION.getFieldName())) {
                version = (Long) ConfigurationUtils.readObject(null, null, dataMap, MetaData.VERSION.getFieldName());
            }
            VersionType versionType = null;
            if (dataMap.containsKey(MetaData.VERSION_TYPE.getFieldName())) {
                versionType = VersionType.fromString(ConfigurationUtils.readStringProperty(null, null, dataMap,
                    MetaData.VERSION_TYPE.getFieldName()));
            }
            IngestDocument ingestDocument =
                new IngestDocument(index, type, id, routing, version, versionType, document);
            ingestDocumentList.add(ingestDocument);
        }
        return ingestDocumentList;
    }
}
