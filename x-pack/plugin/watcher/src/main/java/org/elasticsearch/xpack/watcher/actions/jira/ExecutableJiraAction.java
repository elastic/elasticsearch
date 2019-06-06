/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.jira;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.jira.JiraAccount;
import org.elasticsearch.xpack.watcher.notification.jira.JiraIssue;
import org.elasticsearch.xpack.watcher.notification.jira.JiraService;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ExecutableJiraAction extends ExecutableAction<JiraAction> {

    private final TextTemplateEngine engine;
    private final JiraService jiraService;

    public ExecutableJiraAction(JiraAction action, Logger logger, JiraService jiraService, TextTemplateEngine templateEngine) {
        super(action, logger);
        this.jiraService = jiraService;
        this.engine = templateEngine;
    }

    @Override
    public Action.Result execute(final String actionId, WatchExecutionContext ctx, Payload payload) throws Exception {
        JiraAccount account = jiraService.getAccount(action.account);
        if (account == null) {
            // the account associated with this action was deleted
            throw new IllegalStateException("account [" + action.account + "] was not found. perhaps it was deleted");
        }

        final Function<String, String> render = s -> engine.render(new TextTemplate(s), Variables.createCtxParamsMap(ctx, payload));

        Map<String, Object> fields = new HashMap<>();
        // Apply action fields
        fields = merge(fields, action.fields, render);
        // Apply default fields
        fields = merge(fields, account.getDefaults(), render);

        if (ctx.simulateAction(actionId)) {
            return new JiraAction.Simulated(fields);
        }

        JiraIssue result = account.createIssue(fields, action.proxy);
        return new JiraAction.Executed(result);
    }

    /**
     * Merges the defaults provided as the second parameter into the content of the first
     * while applying a {@link Function} on both map key and map value.
     */
    static Map<String, Object> merge(final Map<String, Object> fields, final Map<String, ?> defaults, final Function<String, String> fn) {
        if (defaults != null) {
            for (Map.Entry<String, ?> defaultEntry : defaults.entrySet()) {
                Object value = defaultEntry.getValue();
                if (value instanceof String) {
                    // Apply the transformation to a simple string
                    value = fn.apply((String) value);

                } else if (value instanceof Map) {
                    // Apply the transformation to a map
                    value = merge(new HashMap<>(), (Map<String, ?>) value, fn);

                } else if (value instanceof String[]) {
                    // Apply the transformation to an array of strings
                    String[] newValues = new String[((String[]) value).length];
                    for (int i = 0; i < newValues.length; i++) {
                        newValues[i] = fn.apply(((String[]) value)[i]);
                    }
                    value = newValues;

                } else if (value instanceof List) {
                    // Apply the transformation to a list of strings
                    List<Object> newValues = new ArrayList<>(((List) value).size());
                    for (Object v : (List) value) {
                        if (v instanceof String) {
                            newValues.add(fn.apply((String) v));
                        } else if (v instanceof Map) {
                            newValues.add(merge(new HashMap<>(), (Map<String, ?>) v, fn));
                        } else {
                            newValues.add(v);
                        }
                    }
                    value = newValues;
                }

                // Apply the transformation to the key
                String key = fn.apply(defaultEntry.getKey());

                // Copy the value directly in the map if it does not exist yet.
                // We don't try to merge maps or list.
                if (fields.containsKey(key) == false) {
                    fields.put(key, value);
                }
            }
        }
        return fields;
    }
}
