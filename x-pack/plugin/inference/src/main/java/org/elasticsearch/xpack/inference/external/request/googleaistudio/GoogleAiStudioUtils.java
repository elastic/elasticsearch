/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.googleaistudio;

public class GoogleAiStudioUtils {

    public static final String HOST_SUFFIX = "generativelanguage.googleapis.com";

    public static final String V1 = "v1";

    public static final String MODELS = "models";

    public static final String GENERATE_CONTENT_ACTION = "generateContent";

    public static final String STREAM_GENERATE_CONTENT_ACTION = "streamGenerateContent";

    public static final String BATCH_EMBED_CONTENTS_ACTION = "batchEmbedContents";

    private GoogleAiStudioUtils() {}

}
