/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import com.github.nitram509.jmacaroons.Macaroon;
import com.github.nitram509.jmacaroons.MacaroonsBuilder;
import com.github.nitram509.jmacaroons.MacaroonsVerifier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class MacaroonService {

    private static final NamedXContentRegistry DEFAULT_NAMED_X_CONTENT_REGISTRY = new NamedXContentRegistry(
        CollectionUtils.concatLists(ClusterModule.getNamedXWriteables(), new SearchModule(Settings.EMPTY, List.of()).getNamedXContents())
    );
    public static final XContentParserConfiguration CONFIG = XContentParserConfiguration.EMPTY.withRegistry(
        DEFAULT_NAMED_X_CONTENT_REGISTRY
    ).withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

    private static final Logger logger = LogManager.getLogger(MacaroonService.class);

    private final String macKey;

    public MacaroonService() {
        macKey = "key";
    }

    public String createMacaroon(String pointInTimeId) {
        logger.info("Creating macaroon for PIT [{}]", pointInTimeId);
        return new MacaroonsBuilder("", macKey, generateId()).add_first_party_caveat(pointInTimeIdCaveat(pointInTimeId))
            .getMacaroon()
            .serialize();
    }

    public boolean isMacaroonValid(String macaroonPayload, String pointInTimeId) {
        return isMacaroonValid(macaroonPayload, pointInTimeId, null);
    }

    public boolean isMacaroonValid(String macaroonPayload, String pointInTimeId, QueryBuilder query) {
        try {
            final Macaroon macaroon = MacaroonsBuilder.deserialize(macaroonPayload);
            final MacaroonsVerifier verifier = new MacaroonsVerifier(macaroon).satisfyExact(pointInTimeIdCaveat(pointInTimeId));
            if (query != null) {
                satisfyQueryCaveat(verifier, query);
            }
            return verifier.isValid(macKey);
        } catch (Exception e) {
            logger.warn("Failed to handle macaroon", e);
            return false;
        }
    }

    private void satisfyQueryCaveat(MacaroonsVerifier verifier, QueryBuilder query) {
        verifier.satisfyGeneral((c) -> {
            try {
                XContentParser parser = JsonXContent.jsonXContent.createParser(CONFIG, c);
                QueryBuilder builder = parseInnerQueryBuilder(parser);
                return builder.equals(query);
            } catch (IOException e) {
                logger.info("Failed parsing", e);
                return false;
            }
        });
    }

    private String extractQueryIfPresent(QueryBuilder query) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            query.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return Strings.toString(builder);
        } catch (IOException e) {
            logger.warn("Failed extracting query", e);
            return null;
        }
    }

    private String pointInTimeIdCaveat(String pointInTimeId) {
        return "pit_id = [%s]".formatted(pointInTimeId);
    }

    private String generateId() {
        return StringHelper.idToString(StringHelper.randomId());
    }
}
