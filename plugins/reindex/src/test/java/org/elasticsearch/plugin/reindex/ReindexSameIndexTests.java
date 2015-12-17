package org.elasticsearch.plugin.reindex;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

import org.elasticsearch.action.ActionRequestValidationException;
import org.junit.Before;

/**
 * Tests that indexing from an index back into itself fails the request.
 */
public class ReindexSameIndexTests extends ReindexTestCase {
    @Before
    public void createIndices() {
        createIndex("target", "target2", "foo", "bar", "baz", "source", "source2");
        ensureGreen();
    }

    // NOCOMMIT make sure to resolve aliases one and use the resolved values!
    public void testObviousCases() throws Exception {
        fails("target", "target");
        fails("target", "foo", "bar", "target", "baz");
        fails("target", "foo", "bar", "target", "baz", "target");
        succeeds("target", "source");
        succeeds("target", "source", "source2");
    }

    public void testAliasesContainTarget() throws Exception {
        assertAcked(client().admin().indices().prepareAliases()
                .addAlias("target", "target_alias")
                .addAlias(new String[] {"target", "target2"}, "target_multi")
                .addAlias(new String[] {"source", "source2"}, "source_multi"));

        fails("target", "target_alias");
        fails("target_alias", "target");
        fails("target", "foo", "bar", "target_alias", "baz");
        fails("target_alias", "foo", "bar", "target_alias", "baz");
        fails("target_alias", "foo", "bar", "target", "baz");
        fails("target", "foo", "bar", "target_alias", "target_alias");
        fails("target", "target_multi");
        fails("target", "foo", "bar", "target_multi", "baz");
        succeeds("target", "source_multi");
        succeeds("target", "source", "source2", "source_multi");
    }

    public void testTargetIsAlias() throws Exception {
        assertAcked(client().admin().indices().prepareAliases()
                .addAlias(new String[] {"target", "target2"}, "target_multi"));

        try {
            newIndexBySearch().source("foo").destination("target_multi").get();
            fail("Expected failure");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Alias [target_multi] has more than one indices associated with it [["));
            // The index names can come in either order
            assertThat(e.getMessage(), containsString("target"));
            assertThat(e.getMessage(), containsString("target2"));
        }
    }

    private void fails(String target, String... sources) throws Exception {
        try {
            newIndexBySearch().source(sources).destination(target).get();
            fail("Expected an exception");
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(),
                    containsString("index-by-search cannot write into an index its reading from [target]"));
        }
    }

    private void succeeds(String target, String... sources) throws Exception {
        newIndexBySearch().source(sources).destination(target).get();
    }
}
