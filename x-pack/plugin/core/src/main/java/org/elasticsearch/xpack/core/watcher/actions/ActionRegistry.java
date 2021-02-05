/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.actions;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.watcher.condition.ConditionRegistry;
import org.elasticsearch.xpack.core.watcher.support.WatcherUtils;
import org.elasticsearch.xpack.core.watcher.transform.TransformRegistry;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ActionRegistry {

    private final Map<String, ActionFactory> parsers;
    private final ConditionRegistry conditionRegistry;
    private final TransformRegistry transformRegistry;
    private final Clock clock;
    private final XPackLicenseState licenseState;

    public ActionRegistry(Map<String, ActionFactory> parsers,
                          ConditionRegistry conditionRegistry, TransformRegistry transformRegistry,
                          Clock clock,
                          XPackLicenseState licenseState) {
        this.parsers = parsers;
        this.conditionRegistry = conditionRegistry;
        this.transformRegistry = transformRegistry;
        this.clock = clock;
        this.licenseState = licenseState;
    }

    ActionFactory factory(String type) {
        return parsers.get(type);
    }

    public List<ActionWrapper> parseActions(String watchId, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("could not parse actions for watch [{}]. expected an object but found [{}] instead",
                    watchId, parser.currentToken());
        }
        List<ActionWrapper> actions = new ArrayList<>();

        String id = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                id = parser.currentName();
                if (WatcherUtils.isValidId(id) == false) {
                    throw new ElasticsearchParseException("could not parse action [{}] for watch [{}]. id contains whitespace", id,
                            watchId);
                }
            } else if (token == XContentParser.Token.START_OBJECT && id != null) {
                actions.add(ActionWrapper.parse(watchId, id, parser, this, clock, licenseState));
            }
        }
        return actions;
    }

    public TransformRegistry getTransformRegistry() {
        return transformRegistry;
    }

    public ConditionRegistry getConditionRegistry() {
        return conditionRegistry;
    }
}
