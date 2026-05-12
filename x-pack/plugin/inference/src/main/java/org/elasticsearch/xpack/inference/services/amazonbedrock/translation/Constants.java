/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.translation;

public class Constants {

    public static final String AUTO_TOOL_CHOICE = "auto";
    public static final String REQUIRED_TOOL_CHOICE = "required";
    public static final String NONE_TOOL_CHOICE = "none";
    public static final String FUNCTION_TYPE = "function";

    public static final String FINISH_REASON_TOOL_CALLS = "tool_calls";
    public static final String FINISH_REASON_LENGTH = "length";
    public static final String FINISH_REASON_CONTENT_FILTER = "content_filter";
    public static final String FINISH_REASON_STOP = "stop";

    private Constants() {}
}
