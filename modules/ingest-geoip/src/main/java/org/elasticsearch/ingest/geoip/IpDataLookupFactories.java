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
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.ingest.geoip.IpinfoIpDataLookups.IPINFO_PREFIX;
import static org.elasticsearch.ingest.geoip.IpinfoIpDataLookups.getIpinfoDatabase;
import static org.elasticsearch.ingest.geoip.IpinfoIpDataLookups.getIpinfoLookup;
import static org.elasticsearch.ingest.geoip.MaxmindIpDataLookups.getMaxmindDatabase;
import static org.elasticsearch.ingest.geoip.MaxmindIpDataLookups.getMaxmindLookup;

final class IpDataLookupFactories {

    private IpDataLookupFactories() {
        // utility class
    }

    interface IpDataLookupFactory {
        IpDataLookup create(List<String> properties);
    }

    /**
     * Parses the passed-in databaseType and return the Database instance that is
     * associated with that databaseType.
     *
     * @param databaseType the database type String from the metadata of the database file
     * @return the Database instance that is associated with the databaseType (or null)
     */
    @Nullable
    static Database getDatabase(final String databaseType) {
        Database database = null;

        if (Strings.hasText(databaseType)) {
            final String databaseTypeLowerCase = databaseType.toLowerCase(Locale.ROOT);
            if (databaseTypeLowerCase.startsWith(IPINFO_PREFIX)) {
                database = getIpinfoDatabase(databaseTypeLowerCase); // all lower case!
            } else {
                // for historical reasons, fall back to assuming maxmind-like type parsing
                database = getMaxmindDatabase(databaseType);
            }
        }

        return database;
    }

    static IpDataLookupFactory get(final String databaseType, final String databaseFile) {
        final Database database = getDatabase(databaseType);
        if (database == null) {
            throw new IllegalArgumentException("Unsupported database type [" + databaseType + "] for file [" + databaseFile + "]");
        }

        final Function<Set<Database.Property>, IpDataLookup> factoryMethod;
        final String databaseTypeLowerCase = databaseType.toLowerCase(Locale.ROOT);
        if (databaseTypeLowerCase.startsWith(IPINFO_PREFIX)) {
            factoryMethod = getIpinfoLookup(database);
        } else {
            // for historical reasons, fall back to assuming maxmind-like types
            factoryMethod = getMaxmindLookup(database);
        }

        if (factoryMethod == null) {
            throw new IllegalArgumentException("Unsupported database type [" + databaseType + "] for file [" + databaseFile + "]");
        }

        return (properties) -> factoryMethod.apply(database.parseProperties(properties));
    }
}
