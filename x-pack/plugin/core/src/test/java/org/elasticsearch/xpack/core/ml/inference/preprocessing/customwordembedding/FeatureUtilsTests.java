/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.CustomWordEmbedding;

import java.io.UnsupportedEncodingException;

import static org.hamcrest.CoreMatchers.equalTo;

public class FeatureUtilsTests extends ESTestCase {

    public void testValidUTF8Length() {
        {
            // Truncate to UTF8 boundary (no cut)
            String strAZ = " a az qalıb breyn rinq intellektual oyunu üzrə yarışın zona mərhələləri " +
                "keçirilib miq un qalıqlarının dənizdən çıxarılması davam edir məhəmməd " +
                "peyğəmbərin karikaturalarını çap edən qəzetin baş redaktoru iş otağında " +
                "ölüb";

            int i = FeatureUtils.validUTF8Length(strAZ, 200);
            assertEquals(200, i);
        }
        {
            // Truncate to UTF8 boundary (cuts)
            String strBE = " а друкаваць іх не было тэхнічна магчыма бліжэй за вільню тым самым часам " +
                "нямецкае кіраўніцтва прапаноўвала апроч ўвядзення лацінкі яе";

            int i = FeatureUtils.validUTF8Length(strBE, 200);
            assertEquals(199, i);
        }
        {
            // Don't truncate
            String strAR = "احتيالية بيع أي حساب";

            int i = FeatureUtils.validUTF8Length(strAR, 200);
            assertEquals(37, i);
        }
        {
            // Truncate to UTF8 boundary (cuts)
            String strZH = "产品的简报和公告 提交该申请后无法进行更改 请确认您的选择是正确的 " +
                "对于要提交的图书 我确认 我是版权所有者或已得到版权所有者的授权 " +
                "要更改您的国家 地区 请在此表的最上端更改您的";

            int i = FeatureUtils.validUTF8Length(strZH, 200);
            assertEquals(198, i);
        }
    }

    public void testFindNumValidBytesToProcess() throws UnsupportedEncodingException {
        // Testing mainly covered by testValidUTF8Length
        String strZH = "产品的简报和公告 提交该申请后无法进行更改 请确认您的选择是正确的 " +
            "对于要提交的图书 我确认 我是版权所有者或已得到版权所有者的授权 " +
            "要更改您的国家 地区 请在此表的最上端更改您的";

        String text = FeatureUtils.truncateToNumValidBytes(strZH, CustomWordEmbedding.MAX_STRING_SIZE_IN_BYTES);
        assertEquals(strZH.length(), text.length());
    }

    public void testCleanText() {
        assertThat(FeatureUtils.cleanAndLowerText("This has a tag in <br> it <ssss>&..///1/2@@3winter"),
            equalTo(" this has a tag in br it ssss winter "));

        assertThat(FeatureUtils.cleanAndLowerText(" This has a tag in <br> it <ssss>&..///1/2@@3winter "),
            equalTo(" this has a tag in br it ssss winter "));

        assertThat(FeatureUtils.cleanAndLowerText(" This has a tag in <p> it </p><ssss>&..///1/2@@3winter "),
            equalTo(" this has a tag in p it p ssss winter "));

        assertThat(FeatureUtils.cleanAndLowerText("  This has a tag in \n<p> it \r\n</p><ssss>&..///1/2@@3winter "),
            equalTo(" this has a tag in p it p ssss winter "));

        assertThat(FeatureUtils.cleanAndLowerText(" !This has    a tag.in\n+|iW£#   <p> hello\nit </p><ssss>&..///1/2@@3winter "),
            equalTo(" this has a tag in iw p hello it p ssss winter "));

        assertThat(FeatureUtils.cleanAndLowerText("北京——。"), equalTo(" 北京 "));
        assertThat(FeatureUtils.cleanAndLowerText("北京——中国共产党已为国家主席习近平或许无限期地继续执政扫清了道路。"),
            equalTo(" 北京 中国共产党已为国家主席习近平或许无限期地继续执政扫清了道路 "));
    }
}
