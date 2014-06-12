/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class SampleSettings implements ToXContent {
    public static final int DEFAULT_NUM_DOCS_PER_SHARD = 50;
    private int numDocsPerShard = DEFAULT_NUM_DOCS_PER_SHARD;
    
    public static final int DEFAULT_MAX_TOKENS_PER_DOC = 1000;
    private int maxTokensParsedPerDocument = DEFAULT_MAX_TOKENS_PER_DOC;
    

    public static final int NO_DUPLICATE_DETECTION = 0;
     private int duplicateParagraphLengthInWords = NO_DUPLICATE_DETECTION;
     
    public static final int DEFAULT_NUM_RECORDED_TOKENS = 50000;    
    int numRecordedTokens = DEFAULT_NUM_RECORDED_TOKENS;
    
    // Note: I experimented with extrapolating stats from the sample
    // e.g. multiplying all foreground freq counts by (totalMatchesOnShard/numDocsPerShard)
    // This was intended to compensate for the fact we were only using a sample
    // but some insignificant rare terms ended up being artificially over-inflated and so 
    // became significant. The net take-away is it doesn't make sense to extrapolate from
    // a sample when we are often looking for the rarer elements in a set.
    //boolean extrapolateStats = false;
    
    // TODO Big documents e.g. Wikipedia articles could potentially be split
    // and considered as multiple documents for the purposes of analyzing
    // frequencies of terms in documents. A setting like the one below could control
    // the boundaries of how TokenStreams are split into multiple docs after
    // N tokens have been produced
    //int wordsPerSplitDoc = -1;
    
    
    public int getNumDocsPerShard() {
        return numDocsPerShard;
    }
    /**
     * Max number of docs to sample on each shard. 
     * The more documents sampled, the slower the query and 
     * also does not always lead to higher quality suggestions 
     * as the sample will typically start to include less 
     * relevant documents in the sample.  
     * 
     * @param numDocsPerShard The maximum number of documents sampled on each shard
     */
    public void setNumDocsPerShard(int numDocsPerShard) {
        //TODO do we want to mandate an upper limit on this setting?
        this.numDocsPerShard = numDocsPerShard;
    }
    

    public int getMaxTokensParsedPerDocument() {
        return maxTokensParsedPerDocument;
    }
    /**
     * The maximum number of tokens sampled from each document.
     * This is principally a performance optimization to avoid 
     * the costs of re-tokenizing very large documents in their 
     * entirety.
     * 
     * @param maxTokensParsedPerDocument The maximum number of terms tokenized from each document.
     */
    public void setMaxTokensParsedPerDocument(int maxTokensParsedPerDocument) {
        this.maxTokensParsedPerDocument = maxTokensParsedPerDocument;
    }
    
    
    public int getDuplicateParagraphLengthInWords() {
        return duplicateParagraphLengthInWords;
    }
    /**
     * Set this value to > 1 to enable removal of duplicate token sequences
     * from sampled documents. For useful, significant term suggestions the content
     * samples should generally be cleansed of repeated sections which can be introduced 
     * by boiler-plate copyright footers, retweets or email replies that automatically 
     * copy the bodies of the mail they reply to.
     * This setting uses a window of historical tokens hashes to identify duplicate token
     * sequences. The window holds hashes of tokens not the original strings so will produce
     * some false positives if duplicateParagraphLengthInWords is set to a low number e.g. 2
     * but should be accurate on longer sequences e.g. 8 tokens 
     * 
     * @param duplicateParagraphLengthInWords The minimum number of tokens seen in the same sequence
     * as previously which are required before the sequence is marked as duplicate content and
     * removed from our analysis sample
     */
    public void setDuplicateParagraphLengthInWords(int duplicateParagraphLengthInWords) {
        //TODO mandate a sensible lower-bound on this? e.g. <=2 not generally useful
        this.duplicateParagraphLengthInWords = duplicateParagraphLengthInWords;
    }
    public int getNumRecordedTokens() {
        return numRecordedTokens;
    }
    
    /**
     * If duplicateParagraphLengthInWords is set to >1 this setting controls the size in bytes of the
     * sliding window used to identify duplicate content. The default setting is 50000 and each token
     * is recorded in hashed form as a single byte.
     * 
     * @param numRecordedTokens size in bytes of the sliding window used to find duplicate content
     */
    public void setNumRecordedTokens(int numRecordedTokens) {
        //TODO do we want to mandate an upper limit on this setting?
        this.numRecordedTokens = numRecordedTokens;
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SignificantTermsParametersParser.SAMPLING_SETTINGS.getPreferredName());
        if (numDocsPerShard != DEFAULT_NUM_DOCS_PER_SHARD) {
            builder.field(SignificantTermsParametersParser.DOCS_PER_SHARD.getPreferredName(), numDocsPerShard);
        }
        if (numRecordedTokens != DEFAULT_NUM_RECORDED_TOKENS) {
            builder.field(SignificantTermsParametersParser.NUM_RECORDED_TOKENS.getPreferredName(), numRecordedTokens);
        }
        if (duplicateParagraphLengthInWords != NO_DUPLICATE_DETECTION) {
            builder.field(SignificantTermsParametersParser.DUPLICATE_PARA_WORD_LENGTH.getPreferredName(), duplicateParagraphLengthInWords);
        }
        if (maxTokensParsedPerDocument != DEFAULT_MAX_TOKENS_PER_DOC) {
            builder.field(SignificantTermsParametersParser.MAX_TOKENS_PARSED_PER_DOC.getPreferredName(), maxTokensParsedPerDocument);
        }

        builder.endObject();
        return builder;
    }
    
}
