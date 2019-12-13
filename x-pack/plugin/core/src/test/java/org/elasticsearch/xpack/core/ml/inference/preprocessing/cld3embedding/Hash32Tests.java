/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class Hash32Tests extends ESTestCase {

    public void testHash32WithDefaultSeed() {
        Hash32 hash32 = new Hash32();

        assertThat(Integer.toUnsignedString(hash32.hash("$")), equalTo("3182025368"));
        assertThat(Integer.toUnsignedString(hash32.hash("t")), equalTo("138132021"));
        assertThat(Integer.toUnsignedString(hash32.hash("i")), equalTo("2965039166"));
        assertThat(Integer.toUnsignedString(hash32.hash("l")), equalTo("1580082402"));
        assertThat(Integer.toUnsignedString(hash32.hash("^")), equalTo("2207093652"));

        assertThat(Integer.toUnsignedString(hash32.hash("in")), equalTo("4112208493"));
        assertThat(Integer.toUnsignedString(hash32.hash("en")), equalTo("2079238153"));
        assertThat(Integer.toUnsignedString(hash32.hash("^$")), equalTo("4032735838"));
        assertThat(Integer.toUnsignedString(hash32.hash("tt")), equalTo("817858275"));
        assertThat(Integer.toUnsignedString(hash32.hash("it")), equalTo("1589737287"));
        assertThat(Integer.toUnsignedString(hash32.hash("n$")), equalTo("2413977248"));

        assertThat(Integer.toUnsignedString(hash32.hash("rit")), equalTo("1921565252"));
        assertThat(Integer.toUnsignedString(hash32.hash("^in")), equalTo("2446331876"));
        assertThat(Integer.toUnsignedString(hash32.hash("tte")), equalTo("1722883625"));
        assertThat(Integer.toUnsignedString(hash32.hash("^is")), equalTo("1526307372"));
        assertThat(Integer.toUnsignedString(hash32.hash("^wr")), equalTo("2206189520"));

        assertThat(Integer.toUnsignedString(hash32.hash("^is$")), equalTo("790119734"));
        assertThat(Integer.toUnsignedString(hash32.hash("glis")), equalTo("3888555831"));
        assertThat(Integer.toUnsignedString(hash32.hash("^tex")), equalTo("260719639"));
        assertThat(Integer.toUnsignedString(hash32.hash("text")), equalTo("1712111248"));
        assertThat(Integer.toUnsignedString(hash32.hash("ritt")), equalTo("3582828510"));

        assertThat(Integer.toUnsignedString(hash32.hash("다")), equalTo("2787769683"));
        assertThat(Integer.toUnsignedString(hash32.hash("세")), equalTo("1500228512"));
        assertThat(Integer.toUnsignedString(hash32.hash("습")), equalTo("2218653723"));
        assertThat(Integer.toUnsignedString(hash32.hash("할")), equalTo("3913427461"));
        assertThat(Integer.toUnsignedString(hash32.hash("여")), equalTo("2765963430"));

        assertThat(Integer.toUnsignedString(hash32.hash("한$")), equalTo("2241076599"));
        assertThat(Integer.toUnsignedString(hash32.hash("다$")), equalTo("1903255056"));
        assertThat(Integer.toUnsignedString(hash32.hash("니다")), equalTo("2071203213"));
        assertThat(Integer.toUnsignedString(hash32.hash("있습")), equalTo("530856195"));
        assertThat(Integer.toUnsignedString(hash32.hash("수$")), equalTo("681457790"));

        assertThat(Integer.toUnsignedString(hash32.hash("포트$")), equalTo("3931486187"));
        assertThat(Integer.toUnsignedString(hash32.hash("습니다")), equalTo("3371415996"));
        assertThat(Integer.toUnsignedString(hash32.hash("^리포")), equalTo("3050042250"));
        assertThat(Integer.toUnsignedString(hash32.hash("권한$")), equalTo("2791953805"));

        assertThat(Integer.toUnsignedString(hash32.hash("습니다$")), equalTo("2290190997"));
        assertThat(Integer.toUnsignedString(hash32.hash("^있습니")), equalTo("2954770380"));
        assertThat(Integer.toUnsignedString(hash32.hash("^권한$")), equalTo("3837435447"));
        assertThat(Integer.toUnsignedString(hash32.hash("부여할$")), equalTo("815437673"));
        assertThat(Integer.toUnsignedString(hash32.hash("^부여할")), equalTo("151570116"));

    }
}
