/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestDocument.Metadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SimulatePipelineRequest extends LegacyActionRequest implements ToXContentObject {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(SimulatePipelineRequest.class);
    private String id;
    private boolean verbose;
    private final BytesReference source;
    private final XContentType xContentType;
    private RestApiVersion restApiVersion;

    /**
     * Creates a new request with the given source and its content type
     */
    public SimulatePipelineRequest(BytesReference source, XContentType xContentType) {
        this(source, xContentType, RestApiVersion.current());
    }

    public SimulatePipelineRequest(BytesReference source, XContentType xContentType, RestApiVersion restApiVersion) {
        this.source = Objects.requireNonNull(source);
        this.xContentType = Objects.requireNonNull(xContentType);
        this.restApiVersion = restApiVersion;
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
        XContentHelper.writeTo(out, xContentType);
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

    record Parsed(Pipeline pipeline, List<IngestDocument> documents, boolean verbose) {
        Parsed(Pipeline pipeline, List<IngestDocument> documents, boolean verbose) {
            this.pipeline = pipeline;
            this.documents = Collections.unmodifiableList(documents);
            this.verbose = verbose;
        }
    }

    static final String SIMULATED_PIPELINE_ID = "_simulate_pipeline";

    static Parsed parseWithPipelineId(
        String pipelineId,
        Map<String, Object> config,
        boolean verbose,
        IngestService ingestService,
        RestApiVersion restApiVersion
    ) {
        if (pipelineId == null) {
            throw new IllegalArgumentException("param [pipeline] is null");
        }
        Pipeline pipeline = ingestService.getPipeline(pipelineId);
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline [" + pipelineId + "] does not exist");
        }
        List<IngestDocument> ingestDocumentList = parseDocs(config, restApiVersion);
        return new Parsed(pipeline, ingestDocumentList, verbose);
    }

    static Parsed parse(Map<String, Object> config, boolean verbose, IngestService ingestService, RestApiVersion restApiVersion)
        throws Exception {
        Map<String, Object> pipelineConfig = ConfigurationUtils.readMap(null, null, config, Fields.PIPELINE);
        Pipeline pipeline = Pipeline.create(
            SIMULATED_PIPELINE_ID,
            pipelineConfig,
            ingestService.getProcessorFactories(),
            ingestService.getScriptService()
        );
        List<IngestDocument> ingestDocumentList = parseDocs(config, restApiVersion);
        return new Parsed(pipeline, ingestDocumentList, verbose);
    }

    private static List<IngestDocument> parseDocs(Map<String, Object> config, RestApiVersion restApiVersion) {
        List<Map<String, Object>> docs = ConfigurationUtils.readList(null, null, config, Fields.DOCS);
        if (docs.isEmpty()) {
            throw new IllegalArgumentException("must specify at least one document in [docs]");
        }
        List<IngestDocument> ingestDocumentList = new ArrayList<>();
        for (Object object : docs) {
            if ((object instanceof Map) == false) {
                throw new IllegalArgumentException("malformed [docs] section, should include an inner object");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> dataMap = (Map<String, Object>) object;
            Map<String, Object> document = ConfigurationUtils.readMap(null, null, dataMap, Fields.SOURCE);
            String index = ConfigurationUtils.readStringOrIntProperty(null, null, dataMap, Metadata.INDEX.getFieldName(), "_index");
            String id = ConfigurationUtils.readStringOrIntProperty(null, null, dataMap, Metadata.ID.getFieldName(), "_id");
            String routing = ConfigurationUtils.readOptionalStringOrIntProperty(null, null, dataMap, Metadata.ROUTING.getFieldName());
            if (restApiVersion == RestApiVersion.V_7 && dataMap.containsKey(Metadata.TYPE.getFieldName())) {
                deprecationLogger.compatibleCritical(
                    "simulate_pipeline_with_types",
                    "[types removal] specifying _type in pipeline simulation requests is deprecated"
                );
            }
            long version = Versions.MATCH_ANY;
            if (dataMap.containsKey(Metadata.VERSION.getFieldName())) {
                String versionValue = ConfigurationUtils.readOptionalStringOrLongProperty(
                    null,
                    null,
                    dataMap,
                    Metadata.VERSION.getFieldName()
                );
                if (versionValue != null) {
                    version = Long.parseLong(versionValue);
                } else {
                    throw new IllegalArgumentException("[_version] cannot be null");
                }
            }
            VersionType versionType = null;
            if (dataMap.containsKey(Metadata.VERSION_TYPE.getFieldName())) {
                versionType = VersionType.fromString(
                    ConfigurationUtils.readStringProperty(null, null, dataMap, Metadata.VERSION_TYPE.getFieldName())
                );
            }
            IngestDocument ingestDocument = new IngestDocument(index, id, version, routing, versionType, document);
            if (dataMap.containsKey(Metadata.IF_SEQ_NO.getFieldName())) {
                String ifSeqNoValue = ConfigurationUtils.readOptionalStringOrLongProperty(
                    null,
                    null,
                    dataMap,
                    Metadata.IF_SEQ_NO.getFieldName()
                );
                if (ifSeqNoValue != null) {
                    Long ifSeqNo = Long.valueOf(ifSeqNoValue);
                    ingestDocument.setFieldValue(Metadata.IF_SEQ_NO.getFieldName(), ifSeqNo);
                } else {
                    throw new IllegalArgumentException("[_if_seq_no] cannot be null");
                }
            }
            if (dataMap.containsKey(Metadata.IF_PRIMARY_TERM.getFieldName())) {
                String ifPrimaryTermValue = ConfigurationUtils.readOptionalStringOrLongProperty(
                    null,
                    null,
                    dataMap,
                    Metadata.IF_PRIMARY_TERM.getFieldName()
                );
                if (ifPrimaryTermValue != null) {
                    Long ifPrimaryTerm = Long.valueOf(ifPrimaryTermValue);
                    ingestDocument.setFieldValue(Metadata.IF_PRIMARY_TERM.getFieldName(), ifPrimaryTerm);
                } else {
                    throw new IllegalArgumentException("[_if_primary_term] cannot be null");
                }
            }
            if (dataMap.containsKey(Metadata.DYNAMIC_TEMPLATES.getFieldName())) {
                Map<String, String> dynamicTemplates = ConfigurationUtils.readMap(
                    null,
                    null,
                    dataMap,
                    Metadata.DYNAMIC_TEMPLATES.getFieldName()
                );
                if (dynamicTemplates != null) {
                    ingestDocument.setFieldValue(Metadata.DYNAMIC_TEMPLATES.getFieldName(), new HashMap<>(dynamicTemplates));
                } else {
                    throw new IllegalArgumentException("[_dynamic_templates] cannot be null");
                }
            }
            ingestDocumentList.add(ingestDocument);
        }
        return ingestDocumentList;
    }

    public RestApiVersion getRestApiVersion() {
        return restApiVersion;
    }
}
