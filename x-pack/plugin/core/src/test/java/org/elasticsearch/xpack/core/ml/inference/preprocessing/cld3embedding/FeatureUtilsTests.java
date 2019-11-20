/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.CLD3WordEmbedding;

import java.io.UnsupportedEncodingException;

import static org.hamcrest.CoreMatchers.equalTo;

public class FeatureUtilsTests extends ESTestCase {

    public void testHash32WithDefaultSeed() throws UnsupportedEncodingException {

        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("$")), equalTo("3182025368"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("t")), equalTo("138132021"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("i")), equalTo("2965039166"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("l")), equalTo("1580082402"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("^")), equalTo("2207093652"));

        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("in")), equalTo("4112208493"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("en")), equalTo("2079238153"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("^$")), equalTo("4032735838"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("tt")), equalTo("817858275"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("it")), equalTo("1589737287"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("n$")), equalTo("2413977248"));

        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("rit")), equalTo("1921565252"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("^in")), equalTo("2446331876"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("tte")), equalTo("1722883625"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("^is")), equalTo("1526307372"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("^wr")), equalTo("2206189520"));

        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("^is$")), equalTo("790119734"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("glis")), equalTo("3888555831"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("^tex")), equalTo("260719639"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("text")), equalTo("1712111248"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("ritt")), equalTo("3582828510"));

        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("다")), equalTo("2787769683"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("세")), equalTo("1500228512"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("습")), equalTo("2218653723"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("할")), equalTo("3913427461"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("여")), equalTo("2765963430"));

        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("한$")), equalTo("2241076599"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("다$")), equalTo("1903255056"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("니다")), equalTo("2071203213"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("있습")), equalTo("530856195"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("수$")), equalTo("681457790"));

        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("포트$")), equalTo("3931486187"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("습니다")), equalTo("3371415996"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("^리포")), equalTo("3050042250"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("권한$")), equalTo("2791953805"));

        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("습니다$")), equalTo("2290190997"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("^있습니")), equalTo("2954770380"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("^권한$")), equalTo("3837435447"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("부여할$")), equalTo("815437673"));
        assertThat(Integer.toUnsignedString(FeatureUtils.Hash32WithDefaultSeed("^부여할")), equalTo("151570116"));

    }

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

        String text = FeatureUtils.truncateToNumValidBytes(strZH, CLD3WordEmbedding.MAX_STRING_SIZE_IN_BYTES);
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
