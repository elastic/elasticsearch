/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.plugins;

import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.ingest.common.RecoverFailureDocumentProcessor;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.ingest.ProcessorFactoryBridge;
import org.elasticsearch.logstashbridge.ingest.ProcessorParametersBridge;

import java.util.Map;

/**
 * An external bridge for {@link IngestCommonPlugin}
 */
public final class IngestCommonPluginBridge implements IngestPluginBridge {

    public static final String APPEND_PROCESSOR_TYPE = org.elasticsearch.ingest.common.AppendProcessor.TYPE;
    public static final String BYTES_PROCESSOR_TYPE = org.elasticsearch.ingest.common.BytesProcessor.TYPE;
    public static final String COMMUNITY_ID_PROCESSOR_TYPE = org.elasticsearch.ingest.common.CommunityIdProcessor.TYPE;
    public static final String CONVERT_PROCESSOR_TYPE = org.elasticsearch.ingest.common.ConvertProcessor.TYPE;
    public static final String CSV_PROCESSOR_TYPE = org.elasticsearch.ingest.common.CsvProcessor.TYPE;
    public static final String DATE_INDEX_NAME_PROCESSOR_TYPE = org.elasticsearch.ingest.common.DateIndexNameProcessor.TYPE;
    public static final String DATE_PROCESSOR_TYPE = org.elasticsearch.ingest.common.DateProcessor.TYPE;
    public static final String DISSECT_PROCESSOR_TYPE = org.elasticsearch.ingest.common.DissectProcessor.TYPE;
    public static final String DROP_PROCESSOR_TYPE = org.elasticsearch.ingest.DropProcessor.TYPE;
    public static final String FAIL_PROCESSOR_TYPE = org.elasticsearch.ingest.common.FailProcessor.TYPE;
    public static final String FINGERPRINT_PROCESSOR_TYPE = org.elasticsearch.ingest.common.FingerprintProcessor.TYPE;
    public static final String FOR_EACH_PROCESSOR_TYPE = org.elasticsearch.ingest.common.ForEachProcessor.TYPE;
    public static final String GROK_PROCESSOR_TYPE = org.elasticsearch.ingest.common.GrokProcessor.TYPE;
    public static final String GSUB_PROCESSOR_TYPE = org.elasticsearch.ingest.common.GsubProcessor.TYPE;
    public static final String HTML_STRIP_PROCESSOR_TYPE = org.elasticsearch.ingest.common.HtmlStripProcessor.TYPE;
    public static final String JOIN_PROCESSOR_TYPE = org.elasticsearch.ingest.common.JoinProcessor.TYPE;
    public static final String JSON_PROCESSOR_TYPE = org.elasticsearch.ingest.common.JsonProcessor.TYPE;
    public static final String KEY_VALUE_PROCESSOR_TYPE = org.elasticsearch.ingest.common.KeyValueProcessor.TYPE;
    public static final String LOWERCASE_PROCESSOR_TYPE = org.elasticsearch.ingest.common.LowercaseProcessor.TYPE;
    public static final String NETWORK_DIRECTION_PROCESSOR_TYPE = org.elasticsearch.ingest.common.NetworkDirectionProcessor.TYPE;
    public static final String REGISTERED_DOMAIN_PROCESSOR_TYPE = org.elasticsearch.ingest.common.RegisteredDomainProcessor.TYPE;
    public static final String RECOVER_FAILURE_DOCUMENT_PROCESSOR_TYPE = RecoverFailureDocumentProcessor.TYPE;
    public static final String REMOVE_PROCESSOR_TYPE = org.elasticsearch.ingest.common.RemoveProcessor.TYPE;
    public static final String RENAME_PROCESSOR_TYPE = org.elasticsearch.ingest.common.RenameProcessor.TYPE;
    public static final String REROUTE_PROCESSOR_TYPE = org.elasticsearch.ingest.common.RerouteProcessor.TYPE;
    public static final String SCRIPT_PROCESSOR_TYPE = org.elasticsearch.ingest.common.ScriptProcessor.TYPE;
    public static final String SET_PROCESSOR_TYPE = org.elasticsearch.ingest.common.SetProcessor.TYPE;
    public static final String SORT_PROCESSOR_TYPE = org.elasticsearch.ingest.common.SortProcessor.TYPE;
    public static final String SPLIT_PROCESSOR_TYPE = org.elasticsearch.ingest.common.SplitProcessor.TYPE;
    public static final String TRIM_PROCESSOR_TYPE = org.elasticsearch.ingest.common.TrimProcessor.TYPE;
    public static final String URL_DECODE_PROCESSOR_TYPE = org.elasticsearch.ingest.common.URLDecodeProcessor.TYPE;
    public static final String UPPERCASE_PROCESSOR_TYPE = org.elasticsearch.ingest.common.UppercaseProcessor.TYPE;
    public static final String URI_PARTS_PROCESSOR_TYPE = org.elasticsearch.ingest.common.UriPartsProcessor.TYPE;

    private final IngestCommonPlugin delegate;

    public IngestCommonPluginBridge() {
        delegate = new IngestCommonPlugin();
    }

    @Override
    public Map<String, ProcessorFactoryBridge> getProcessors(final ProcessorParametersBridge parameters) {
        return StableBridgeAPI.fromInternal(this.delegate.getProcessors(parameters.toInternal()), ProcessorFactoryBridge::fromInternal);
    }
}
