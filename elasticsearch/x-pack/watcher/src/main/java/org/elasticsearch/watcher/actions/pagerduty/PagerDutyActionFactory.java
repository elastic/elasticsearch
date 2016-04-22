/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.pagerduty;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.ActionFactory;
import org.elasticsearch.watcher.actions.hipchat.ExecutableHipChatAction;
import org.elasticsearch.xpack.notification.pagerduty.PagerDutyAccount;
import org.elasticsearch.xpack.notification.pagerduty.PagerDutyService;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;

import java.io.IOException;

/**
 *
 */
public class PagerDutyActionFactory extends ActionFactory<PagerDutyAction, ExecutablePagerDutyAction> {

    private final TextTemplateEngine templateEngine;
    private final PagerDutyService pagerDutyService;

    @Inject
    public PagerDutyActionFactory(Settings settings, TextTemplateEngine templateEngine, PagerDutyService pagerDutyService) {
        super(Loggers.getLogger(ExecutableHipChatAction.class, settings));
        this.templateEngine = templateEngine;
        this.pagerDutyService = pagerDutyService;
    }

    @Override
    public String type() {
        return PagerDutyAction.TYPE;
    }

    @Override
    public PagerDutyAction parseAction(String watchId, String actionId, XContentParser parser) throws IOException {
        PagerDutyAction action = PagerDutyAction.parse(watchId, actionId, parser);
        PagerDutyAccount account = pagerDutyService.getAccount(action.event.account);
        if (account == null) {
            throw new ElasticsearchParseException("could not parse [pagerduty] action [{}/{}]. unknown pager duty account [{}]", watchId,
                    account, action.event.account);
        }
        return action;
    }

    @Override
    public ExecutablePagerDutyAction createExecutable(PagerDutyAction action) {
        return new ExecutablePagerDutyAction(action, actionLogger, pagerDutyService, templateEngine);
    }

}
