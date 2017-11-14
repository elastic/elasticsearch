/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.slack.message;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;

public interface MessageElement extends ToXContentObject {

    interface XField {
        ParseField TITLE = new ParseField("title");
        ParseField TEXT = new ParseField("text");
    }
}
