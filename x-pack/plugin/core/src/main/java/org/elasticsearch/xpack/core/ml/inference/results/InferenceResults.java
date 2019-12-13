/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.ingest.IngestDocument;

public interface InferenceResults extends NamedWriteable {

    void writeResult(IngestDocument document, String parentResultField);

}
