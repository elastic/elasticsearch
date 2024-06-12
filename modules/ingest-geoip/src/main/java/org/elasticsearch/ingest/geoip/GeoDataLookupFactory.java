/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import java.util.Set;

@FunctionalInterface
interface GeoDataLookupFactory<PROVIDER extends GeoDataLookup> {
    PROVIDER create(Set<Database.Property> properties);

    static GeoDataLookupFactory<?> get(final Database database) {
        return switch (database) {
            case City -> MaxmindGeoDataLookups.City::new;
            case Country -> MaxmindGeoDataLookups.Country::new;
            case Asn -> MaxmindGeoDataLookups.Asn::new;
            case AnonymousIp -> MaxmindGeoDataLookups.AnonymousIp::new;
            case ConnectionType -> MaxmindGeoDataLookups.ConnectionType::new;
            case Domain -> MaxmindGeoDataLookups.Domain::new;
            case Enterprise -> MaxmindGeoDataLookups.Enterprise::new;
            case Isp -> MaxmindGeoDataLookups.Isp::new;
        };
    }
}
