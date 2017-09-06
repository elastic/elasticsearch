package org.elasticsearch.search.sort;

import static org.hamcrest.Matchers.stringContainsInOrder;

import java.util.Arrays;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

public class FieldSortBuilderTests extends ESTestCase {

    private static final String FIELDNAME = "myfield";
    private static final String QUOTES = "\"";

    public void testToXContent() throws Exception {
        FieldSortBuilder fieldSortBuilder = prepareFSB();
        XContentBuilder xc = XContentFactory.contentBuilder(XContentType.JSON);
        xc.startObject();
        fieldSortBuilder.toXContent(xc, FieldSortBuilder.EMPTY_PARAMS);
        xc.endObject();

        assertThat(xc.string(), stringContainsInOrder(
                Arrays.asList(QUOTES + FIELDNAME + QUOTES, QUOTES + "order" + QUOTES, QUOTES + SortOrder.ASC.toString() + QUOTES)));
    }

    public void testToString() throws Exception {
        FieldSortBuilder fsb = prepareFSB();

        String fsbPrint = fsb.toString();

        assertThat(fsbPrint, stringContainsInOrder(
                Arrays.asList(QUOTES + FIELDNAME + QUOTES, QUOTES + "order" + QUOTES, QUOTES + SortOrder.ASC.toString() + QUOTES)));
    }

    private FieldSortBuilder prepareFSB() {
        FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort(FIELDNAME);
        fieldSortBuilder.order(SortOrder.ASC);
        return fieldSortBuilder;
    }

}
