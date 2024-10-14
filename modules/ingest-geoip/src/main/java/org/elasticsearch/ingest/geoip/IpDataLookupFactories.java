/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

final class IpDataLookupFactories {

    private IpDataLookupFactories() {
        // utility class
    }

    interface IpDataLookupFactory {
        IpDataLookup create(List<String> properties);
    }

    private static final String CITY_DB_SUFFIX = "-City";
    private static final String COUNTRY_DB_SUFFIX = "-Country";
    private static final String ASN_DB_SUFFIX = "-ASN";
    private static final String ANONYMOUS_IP_DB_SUFFIX = "-Anonymous-IP";
    private static final String CONNECTION_TYPE_DB_SUFFIX = "-Connection-Type";
    private static final String DOMAIN_DB_SUFFIX = "-Domain";
    private static final String ENTERPRISE_DB_SUFFIX = "-Enterprise";
    private static final String ISP_DB_SUFFIX = "-ISP";

    @Nullable
    private static Database getMaxmindDatabase(final String databaseType) {
        if (databaseType.endsWith(CITY_DB_SUFFIX)) {
            return Database.City;
        } else if (databaseType.endsWith(COUNTRY_DB_SUFFIX)) {
            return Database.Country;
        } else if (databaseType.endsWith(ASN_DB_SUFFIX)) {
            return Database.Asn;
        } else if (databaseType.endsWith(ANONYMOUS_IP_DB_SUFFIX)) {
            return Database.AnonymousIp;
        } else if (databaseType.endsWith(CONNECTION_TYPE_DB_SUFFIX)) {
            return Database.ConnectionType;
        } else if (databaseType.endsWith(DOMAIN_DB_SUFFIX)) {
            return Database.Domain;
        } else if (databaseType.endsWith(ENTERPRISE_DB_SUFFIX)) {
            return Database.Enterprise;
        } else if (databaseType.endsWith(ISP_DB_SUFFIX)) {
            return Database.Isp;
        } else {
            return null; // no match was found
        }
    }

    /**
     * Parses the passed-in databaseType and return the Database instance that is
     * associated with that databaseType.
     *
     * @param databaseType the database type String from the metadata of the database file
     * @return the Database instance that is associated with the databaseType
     */
    @Nullable
    static Database getDatabase(final String databaseType) {
        Database database = null;

        if (Strings.hasText(databaseType)) {
            database = getMaxmindDatabase(databaseType);
        }

        return database;
    }

    @Nullable
    static Function<Set<Database.Property>, IpDataLookup> getMaxmindLookup(final Database database) {
        return switch (database) {
            case City -> MaxmindIpDataLookups.City::new;
            case Country -> MaxmindIpDataLookups.Country::new;
            case Asn -> MaxmindIpDataLookups.Asn::new;
            case AnonymousIp -> MaxmindIpDataLookups.AnonymousIp::new;
            case ConnectionType -> MaxmindIpDataLookups.ConnectionType::new;
            case Domain -> MaxmindIpDataLookups.Domain::new;
            case Enterprise -> MaxmindIpDataLookups.Enterprise::new;
            case Isp -> MaxmindIpDataLookups.Isp::new;
            default -> null;
        };
    }

    static IpDataLookupFactory get(final String databaseType, final String databaseFile) {
        final Database database = getDatabase(databaseType);
        if (database == null) {
            throw new IllegalArgumentException("Unsupported database type [" + databaseType + "] for file [" + databaseFile + "]");
        }

        final Function<Set<Database.Property>, IpDataLookup> factoryMethod = getMaxmindLookup(database);

        if (factoryMethod == null) {
            throw new IllegalArgumentException("Unsupported database type [" + databaseType + "] for file [" + databaseFile + "]");
        }

        return (properties) -> factoryMethod.apply(database.parseProperties(properties));
    }
}
