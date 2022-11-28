/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.user.ActionUser;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Optional;
import java.util.function.Function;

@Plugin(category = PatternConverter.CATEGORY, name = "ActionUserConverter")
@ConverterKeys({ "action_user" })
public final class ActionUserConverter extends LogEventPatternConverter {

    private static final SetOnce<Function<ThreadContext, Optional<ActionUser>>> ACTION_USER_RESOLVER = new SetOnce<>();

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static ActionUserConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new ActionUserConverter();
    }

    public ActionUserConverter() {
        super("action_user", "action_user");
    }

    public static void setActionUserResolver(Function<ThreadContext, Optional<ActionUser>> resolver) {
        ACTION_USER_RESOLVER.set(resolver);
    }

    public static ActionUser getActionUser() {
        final Function<ThreadContext, Optional<ActionUser>> resolver = ACTION_USER_RESOLVER.get();
        if (resolver == null) {
            return null;
        }
        return HeaderWarning.THREAD_CONTEXT.stream().map(resolver).filter(Optional::isPresent).map(Optional::get).findFirst().orElse(null);
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        final ActionUser actionUser = getActionUser();
        if (actionUser != null) {
            toAppendTo.append(actionUser.getClass().getName());
        }
    }

}
