/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.aggregations;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.join.query.ParentChildTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Small base test-class which combines stuff used for Children and Parent aggregation tests
 */
public abstract class AbstractParentChildTestCase extends ParentChildTestCase {
    protected final Map<String, Control> categoryToControl = new HashMap<>();
    protected final Map<String, ParentControl> articleToControl = new HashMap<>();

    @Before
    public void setupCluster() throws Exception {
        assertAcked(
            prepareCreate("test")
                .setMapping(
                    addFieldMappings(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "article", "comment"),
                        "commenter", "keyword", "category", "keyword"))
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        String[] uniqueCategories = new String[randomIntBetween(1, 25)];
        for (int i = 0; i < uniqueCategories.length; i++) {
            uniqueCategories[i] = Integer.toString(i);
        }
        int catIndex = 0;

        int numParentDocs = randomIntBetween(uniqueCategories.length, uniqueCategories.length * 5);
        for (int i = 0; i < numParentDocs; i++) {
            String id = "article-" + i;

            // TODO: this array is always of length 1, and testChildrenAggs fails if this is changed
            String[] categories = new String[randomIntBetween(1,1)];
            for (int j = 0; j < categories.length; j++) {
                String category = categories[j] = uniqueCategories[catIndex++ % uniqueCategories.length];
                Control control = categoryToControl.computeIfAbsent(category, Control::new);
                control.articleIds.add(id);
                articleToControl.put(id, new ParentControl(category));
            }

            IndexRequestBuilder indexRequest = createIndexRequest("test", "article", id, null, "category", categories, "randomized", true);
            requests.add(indexRequest);
        }

        String[] commenters = new String[randomIntBetween(5, 50)];
        for (int i = 0; i < commenters.length; i++) {
            commenters[i] = Integer.toString(i);
        }

        int id = 0;
        for (Control control : categoryToControl.values()) {
            for (String articleId : control.articleIds) {
                int numChildDocsPerParent = randomIntBetween(0, 5);
                for (int i = 0; i < numChildDocsPerParent; i++) {
                    String commenter = commenters[id % commenters.length];
                    String idValue = "comment-" + id++;
                    control.commentIds.add(idValue);
                    Set<String> ids = control.commenterToCommentId.computeIfAbsent(commenter, k -> new HashSet<>());
                    ids.add(idValue);

                    articleToControl.get(articleId).commentIds.add(idValue);

                    IndexRequestBuilder indexRequest = createIndexRequest("test", "comment", idValue,
                        articleId, "commenter", commenter, "randomized", true);
                    requests.add(indexRequest);
                }
            }
        }

        requests.add(createIndexRequest("test", "article", "a", null, "category", new String[]{"a"}, "randomized", false));
        requests.add(createIndexRequest("test", "article", "b", null, "category", new String[]{"a", "b"}, "randomized", false));
        requests.add(createIndexRequest("test", "article", "c", null, "category", new String[]{"a", "b", "c"}, "randomized", false));
        requests.add(createIndexRequest("test", "article", "d", null, "category", new String[]{"c"}, "randomized", false));
        requests.add(createIndexRequest("test", "comment", "e", "a"));
        requests.add(createIndexRequest("test", "comment", "f", "c"));

        indexRandom(true, requests);
        ensureSearchable("test");
    }


    protected static final class Control {

        final String category;
        final Set<String> articleIds = new HashSet<>();
        final Set<String> commentIds = new HashSet<>();
        final Map<String, Set<String>> commenterToCommentId = new HashMap<>();

        private Control(String category) {
            this.category = category;
        }
    }

    protected static final class ParentControl {
        final String category;
        final Set<String> commentIds = new HashSet<>();

        private ParentControl(String category) {
            this.category = category;
        }
    }
}
