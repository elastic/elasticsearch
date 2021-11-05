/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class Hash32Tests extends ESTestCase {

    public void testHash32WithDefaultSeed() {
        Hash32 hash32 = new Hash32();

        assertThat(hash32.hash("$"), equalTo(3182025368L));
        assertThat(hash32.hash("t"), equalTo(138132021L));
        assertThat(hash32.hash("i"), equalTo(2965039166L));
        assertThat(hash32.hash("l"), equalTo(1580082402L));
        assertThat(hash32.hash("^"), equalTo(2207093652L));

        assertThat(hash32.hash("in"), equalTo(4112208493L));
        assertThat(hash32.hash("en"), equalTo(2079238153L));
        assertThat(hash32.hash("^$"), equalTo(4032735838L));
        assertThat(hash32.hash("tt"), equalTo(817858275L));
        assertThat(hash32.hash("it"), equalTo(1589737287L));
        assertThat(hash32.hash("n$"), equalTo(2413977248L));

        assertThat(hash32.hash("rit"), equalTo(1921565252L));
        assertThat(hash32.hash("^in"), equalTo(2446331876L));
        assertThat(hash32.hash("tte"), equalTo(1722883625L));
        assertThat(hash32.hash("^is"), equalTo(1526307372L));
        assertThat(hash32.hash("^wr"), equalTo(2206189520L));

        assertThat(hash32.hash("^is$"), equalTo(790119734L));
        assertThat(hash32.hash("glis"), equalTo(3888555831L));
        assertThat(hash32.hash("^tex"), equalTo(260719639L));
        assertThat(hash32.hash("text"), equalTo(1712111248L));
        assertThat(hash32.hash("ritt"), equalTo(3582828510L));

        assertThat(hash32.hash("다"), equalTo(2787769683L));
        assertThat(hash32.hash("세"), equalTo(1500228512L));
        assertThat(hash32.hash("습"), equalTo(2218653723L));
        assertThat(hash32.hash("할"), equalTo(3913427461L));
        assertThat(hash32.hash("여"), equalTo(2765963430L));

        assertThat(hash32.hash("한$"), equalTo(2241076599L));
        assertThat(hash32.hash("다$"), equalTo(1903255056L));
        assertThat(hash32.hash("니다"), equalTo(2071203213L));
        assertThat(hash32.hash("있습"), equalTo(530856195L));
        assertThat(hash32.hash("수$"), equalTo(681457790L));

        assertThat(hash32.hash("포트$"), equalTo(3931486187L));
        assertThat(hash32.hash("습니다"), equalTo(3371415996L));
        assertThat(hash32.hash("^리포"), equalTo(3050042250L));
        assertThat(hash32.hash("권한$"), equalTo(2791953805L));

        assertThat(hash32.hash("습니다$"), equalTo(2290190997L));
        assertThat(hash32.hash("^있습니"), equalTo(2954770380L));
        assertThat(hash32.hash("^권한$"), equalTo(3837435447L));
        assertThat(hash32.hash("부여할$"), equalTo(815437673L));
        assertThat(hash32.hash("^부여할"), equalTo(151570116L));

    }
}
