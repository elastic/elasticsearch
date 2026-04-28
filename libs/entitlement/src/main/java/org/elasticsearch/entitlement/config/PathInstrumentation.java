/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.rules.TypeToken;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchService;
import java.util.List;
import java.util.stream.StreamSupport;

public class PathInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        // Path implementations are platform-specific, so we need to dynamically determine the class names
        List<? extends Class<? extends Path>> pathClasses = StreamSupport.stream(
            FileSystems.getDefault().getRootDirectories().spliterator(),
            false
        ).map(Path::getClass).distinct().toList();

        builder.on(pathClasses, rule -> {
            rule.calling(Path::toRealPath, LinkOption[].class).enforce((path, options) -> {
                boolean followLinks = true;
                for (LinkOption option : options) {
                    if (option == LinkOption.NOFOLLOW_LINKS) {
                        followLinks = false;
                    }
                }
                return Policies.fileReadWithLinks(path, followLinks);
            }).elseThrow(IOException::new);
            rule.calling(Path::register, TypeToken.of(WatchService.class), new TypeToken<WatchEvent.Kind<?>[]>() {})
                .enforce(Policies::fileRead)
                .elseThrow(IOException::new);
            rule.calling(
                Path::register,
                TypeToken.of(WatchService.class),
                new TypeToken<WatchEvent.Kind<?>[]>() {},
                TypeToken.of(WatchEvent.Modifier[].class)
            ).enforce(Policies::fileRead).elseThrow(IOException::new);
        });
    }
}
