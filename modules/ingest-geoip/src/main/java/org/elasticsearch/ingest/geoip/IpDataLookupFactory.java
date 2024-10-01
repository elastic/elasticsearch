/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import java.util.Set;

@FunctionalInterface
interface IpDataLookupFactory<PROVIDER extends IpDataLookup> {

    PROVIDER create(Set<Database.Property> properties);

    static IpDataLookupFactory<?> get(final Database database) {
        return switch (database) {
            case City -> MaxmindIpDataLookups.City::new;
            case Country -> MaxmindIpDataLookups.Country::new;
            case Asn -> MaxmindIpDataLookups.Asn::new;
            case AnonymousIp -> MaxmindIpDataLookups.AnonymousIp::new;
            case ConnectionType -> MaxmindIpDataLookups.ConnectionType::new;
            case Domain -> MaxmindIpDataLookups.Domain::new;
            case Enterprise -> MaxmindIpDataLookups.Enterprise::new;
            case Isp -> MaxmindIpDataLookups.Isp::new;
        };
    }
}
