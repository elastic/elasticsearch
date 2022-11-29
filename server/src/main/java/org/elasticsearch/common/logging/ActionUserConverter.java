/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.elasticsearch.action.support.user.ActionUser;
import org.elasticsearch.action.support.user.ActionUserContext;

import java.util.Optional;

public abstract class ActionUserConverter extends LogEventPatternConverter {

    public ActionUserConverter(String name) {
        super(name, name);
    }

    private static Optional<ActionUser> getActionUser() {
        return HeaderWarning.THREAD_CONTEXT.stream()
            .map(ActionUserContext::getEffectiveUser)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .findFirst();
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        getActionUser().ifPresent(user -> doFormat(user, event, toAppendTo));
    }

    protected abstract void doFormat(ActionUser user, LogEvent event, StringBuilder toAppendTo);
}
