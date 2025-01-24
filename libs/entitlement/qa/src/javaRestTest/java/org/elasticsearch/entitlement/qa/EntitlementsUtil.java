/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.cluster.local.PluginInstallSpec;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

class EntitlementsUtil {

    static final CheckedConsumer<XContentBuilder, IOException> ALLOWED_ENTITLEMENTS = builder -> {
        builder.value("create_class_loader");
        builder.value("set_https_connection_properties");
        builder.value("inbound_network");
        builder.value("outbound_network");
        builder.value("load_native_libraries");
        builder.value(
            Map.of(
                "write_system_properties",
                Map.of("properties", List.of("es.entitlements.checkSetSystemProperty", "es.entitlements.checkClearSystemProperty"))
            )
        );
    };

    static void setupEntitlements(PluginInstallSpec spec, boolean modular, CheckedConsumer<XContentBuilder, IOException> policyBuilder) {
        String moduleName = modular ? "org.elasticsearch.entitlement.qa.test" : "ALL-UNNAMED";
        if (policyBuilder != null) {
            try {
                try (var builder = YamlXContent.contentBuilder()) {
                    builder.startObject();
                    builder.field(moduleName);
                    builder.startArray();
                    policyBuilder.accept(builder);
                    builder.endArray();
                    builder.endObject();

                    String policy = Strings.toString(builder);
                    System.out.println("Using entitlement policy:\n" + policy);
                    spec.withEntitlementsOverride(old -> Resource.fromString(policy));
                }

            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        if (modular == false) {
            spec.withPropertiesOverride(old -> {
                String props = old.replace("modulename=org.elasticsearch.entitlement.qa.test", "");
                System.out.println("Using plugin properties:\n" + props);
                return Resource.fromString(props);
            });
        }
    }

    private EntitlementsUtil() {}
}
