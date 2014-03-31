package org.apache.lucene.search.suggest.analyzing;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// TODO
//   - test w/ syns
//   - add pruning of low-freq ngrams?
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
//import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Sort;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.*;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.SpecialOperations;
import org.apache.lucene.util.fst.*;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.Util.MinResult;
import org.elasticsearch.common.collect.HppcMaps;

/**
 * Builds an ngram model from the text sent to {@link
 * #build} and predicts based on the last grams-1 tokens in
 * the request sent to {@link #lookup}.  This tries to
 * handle the "long tail" of suggestions for when the
 * incoming query is a never before seen query string.
 *
 * <p>Likely this suggester would only be used as a
 * fallback, when the primary suggester fails to find
 * any suggestions.
 *
 * <p>Note that the weight for each suggestion is unused,
 * and the suggestions are the analyzed forms (so your
 * analysis process should normally be very "light").
 *
 * <p>This uses the stupid backoff language model to smooth
 * scores across ngram models; see
 * "Large language models in machine translation",
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.76.1126
 * for details.
 *
 * <p> From {@link #lookup}, the key of each result is the
 * ngram token; the value is Long.MAX_VALUE * score (fixed
 * point, cast to long).  Divide by Long.MAX_VALUE to get
 * the score back, which ranges from 0.0 to 1.0.
 *
 * onlyMorePopular is unused.
 *
 * @lucene.experimental
 */
public class XFreeTextSuggester extends Lookup {

  /** Codec name used in the header for the saved model. */
  public final static String CODEC_NAME = "freetextsuggest";

  /** Initial version of the the saved model file format. */
  public final static int VERSION_START = 0;

  /** Current version of the the saved model file format. */
  public final static int VERSION_CURRENT = VERSION_START;

  /** By default we use a bigram model. */
  public static final int DEFAULT_GRAMS = 2;

  // In general this could vary with gram, but the
  // original paper seems to use this constant:
  /** The constant used for backoff smoothing; during
   *  lookup, this means that if a given trigram did not
   *  occur, and we backoff to the bigram, the overall score
   *  will be 0.4 times what the bigram model would have
   *  assigned. */
  public final static double ALPHA = 0.4;

  /** Holds 1gram, 2gram, 3gram models as a single FST. */
  private FST<Long> fst;

  /**
   * Analyzer that will be used for analyzing suggestions at
   * index time.
   */
  private final Analyzer indexAnalyzer;

  private long totTokens;

  /**
   * Analyzer that will be used for analyzing suggestions at
   * query time.
   */
  private final Analyzer queryAnalyzer;

  // 2 = bigram, 3 = trigram
  private final int grams;

  private final byte separator;

  /** Number of entries the lookup was built with */
  private long count = 0;

  /** The default character used to join multiple tokens
   *  into a single ngram token.  The input tokens produced
   *  by the analyzer must not contain this character. */
  public static final byte DEFAULT_SEPARATOR = 0x1e;

  /** Instantiate, using the provided analyzer for both
   *  indexing and lookup, using bigram model by default. */
  public XFreeTextSuggester(Analyzer analyzer) {
    this(analyzer, analyzer, DEFAULT_GRAMS);
  }

  /** Instantiate, using the provided indexing and lookup
   *  analyzers, using bigram model by default. */
  public XFreeTextSuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer) {
    this(indexAnalyzer, queryAnalyzer, DEFAULT_GRAMS);
  }

  /** Instantiate, using the provided indexing and lookup
   *  analyzers, with the specified model (2
   *  = bigram, 3 = trigram, etc.). */
  public XFreeTextSuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer, int grams) {
    this(indexAnalyzer, queryAnalyzer, grams, DEFAULT_SEPARATOR);
  }

  /** Instantiate, using the provided indexing and lookup
   *  analyzers, and specified model (2 = bigram, 3 =
   *  trigram ,etc.).  The separator is passed to {@link
   *  ShingleFilter#setTokenSeparator} to join multiple
   *  tokens into a single ngram token; it must be an ascii
   *  (7-bit-clean) byte.  No input tokens should have this
   *  byte, otherwise {@code IllegalArgumentException} is
   *  thrown. */
  public XFreeTextSuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer, int grams, byte separator) {
    this.grams = grams;
    this.indexAnalyzer = addShingles(indexAnalyzer);
    this.queryAnalyzer = addShingles(queryAnalyzer);
    if (grams < 1) {
      throw new IllegalArgumentException("grams must be >= 1");
    }
    if ((separator & 0x80) != 0) {
      throw new IllegalArgumentException("separator must be simple ascii character");
    }
    this.separator = separator;
  }

  public XFreeTextSuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer, int grams, byte separator,
                              FST<Long> fst, long totalTokens, long count) {
    this(indexAnalyzer, queryAnalyzer, grams, separator);
    this.fst = fst;
    this.totTokens = totalTokens;
    this.count = count;
  }

    /** Returns byte size of the underlying FST. */
  @Override
  public long sizeInBytes() {
    if (fst == null) {
      return 0;
    }
    return fst.sizeInBytes();
  }

  private static class AnalyzingComparator implements Comparator<BytesRef> {

    private final ByteArrayDataInput readerA = new ByteArrayDataInput();
    private final ByteArrayDataInput readerB = new ByteArrayDataInput();
    private final BytesRef scratchA = new BytesRef();
    private final BytesRef scratchB = new BytesRef();

    @Override
    public int compare(BytesRef a, BytesRef b) {
      readerA.reset(a.bytes, a.offset, a.length);
      readerB.reset(b.bytes, b.offset, b.length);

      // By token:
      scratchA.length = readerA.readShort();
      scratchA.bytes = a.bytes;
      scratchA.offset = readerA.getPosition();

      scratchB.bytes = b.bytes;
      scratchB.length = readerB.readShort();
      scratchB.offset = readerB.getPosition();

      int cmp = scratchA.compareTo(scratchB);
      if (cmp != 0) {
        return cmp;
      }
      readerA.skipBytes(scratchA.length);
      readerB.skipBytes(scratchB.length);

      // By length (smaller surface forms sorted first):
      cmp = a.length - b.length;
      if (cmp != 0) {
        return cmp;
      }

      // By surface form:
      scratchA.offset = readerA.getPosition();
      scratchA.length = a.length - scratchA.offset;
      scratchB.offset = readerB.getPosition();
      scratchB.length = b.length - scratchB.offset;

      return scratchA.compareTo(scratchB);
    }
  }

  private Analyzer addShingles(final Analyzer other) {
    if (grams == 1) {
      return other;
    } else {
      // TODO: use ShingleAnalyzerWrapper?
      // Tack on ShingleFilter to the end, to generate token ngrams:
      return new AnalyzerWrapper(other.getReuseStrategy()) {
        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
          return other;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
          ShingleFilter shingles = new ShingleFilter(components.getTokenStream(), 2, grams);
          shingles.setTokenSeparator(Character.toString((char) separator));
          return new TokenStreamComponents(components.getTokenizer(), shingles);
        }
      };
    }
  }

  @Override
  public void build(InputIterator iterator) throws IOException {
    build(iterator, IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB);
  }

  /** Build the suggest index, using up to the specified
   *  amount of temporary RAM while building.  Note that
   *  the weights for the suggestions are ignored. */
  public void build(InputIterator iterator, double ramBufferSizeMB) throws IOException {
    if (iterator.hasPayloads()) {
      throw new IllegalArgumentException("payloads are not supported");
    }

    String prefix = getClass().getSimpleName();
    File directory = Sort.defaultTempDir();
    // TODO: messy ... java7 has Files.createTempDirectory
    // ... but 4.x is java6:
    File tempIndexPath = null;
    Random random = new Random();
    while (true) {
      tempIndexPath = new File(directory, prefix + ".index." + random.nextInt(Integer.MAX_VALUE));
      if (tempIndexPath.mkdir()) {
        break;
      }
    }

    Directory dir = FSDirectory.open(tempIndexPath);

    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_CURRENT, indexAnalyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setRAMBufferSizeMB(ramBufferSizeMB);
    IndexWriter writer = new IndexWriter(dir, iwc);

    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    // TODO: if only we had IndexOptions.TERMS_ONLY...
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    ft.setOmitNorms(true);
    ft.freeze();

    Document doc = new Document();
    Field field = new Field("body", "", ft);
    doc.add(field);

    totTokens = 0;
    IndexReader reader = null;

    boolean success = false;
    count = 0;
    try {
      while (true) {
        BytesRef surfaceForm = iterator.next();
        if (surfaceForm == null) {
          break;
        }
        field.setStringValue(surfaceForm.utf8ToString());
        writer.addDocument(doc);
        count++;
      }
      reader = DirectoryReader.open(writer, false);

      Terms terms = MultiFields.getTerms(reader, "body");
      if (terms == null) {
        throw new IllegalArgumentException("need at least one suggestion");
      }

      // Move all ngrams into an FST:
      TermsEnum termsEnum = terms.iterator(null);

      Outputs<Long> outputs = PositiveIntOutputs.getSingleton();
      Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);

      IntsRef scratchInts = new IntsRef();
      while (true) {
        BytesRef term = termsEnum.next();
        if (term == null) {
          break;
        }
        int ngramCount = countGrams(term);
        if (ngramCount > grams) {
          throw new IllegalArgumentException("tokens must not contain separator byte; got token=" + term + " but gramCount=" + ngramCount + ", which is greater than expected max ngram size=" + grams);
        }
        if (ngramCount == 1) {
          totTokens += termsEnum.totalTermFreq();
        }

        builder.add(Util.toIntsRef(term, scratchInts), encodeWeight(termsEnum.totalTermFreq()));
      }

      fst = builder.finish();
      if (fst == null) {
        throw new IllegalArgumentException("need at least one suggestion");
      }
      //System.out.println("FST: " + fst.getNodeCount() + " nodes");

      /*
      PrintWriter pw = new PrintWriter("/x/tmp/out.dot");
      Util.toDot(fst, pw, true, true);
      pw.close();
      */

      success = true;
    } finally {
      try {
        if (success) {
          IOUtils.close(writer, reader);
        } else {
          IOUtils.closeWhileHandlingException(writer, reader);
        }
      } finally {
        for(String file : dir.listAll()) {
          File path = new File(tempIndexPath, file);
          if (path.delete() == false) {
            throw new IllegalStateException("failed to remove " + path);
          }
        }

        if (tempIndexPath.delete() == false) {
          throw new IllegalStateException("failed to remove " + tempIndexPath);
        }

        dir.close();
      }
    }
  }

  @Override
  public boolean store(DataOutput output) throws IOException {
    CodecUtil.writeHeader(output, CODEC_NAME, VERSION_CURRENT);
    output.writeVLong(count);
    output.writeByte(separator);
    output.writeVInt(grams);
    output.writeVLong(totTokens);
    fst.save(output);
    return true;
  }

  @Override
  public boolean load(DataInput input) throws IOException {
    CodecUtil.checkHeader(input, CODEC_NAME, VERSION_START, VERSION_START);
    count = input.readVLong();
    byte separatorOrig = input.readByte();
    if (separatorOrig != separator) {
        throw new IllegalStateException("separator=" + separator + " is incorrect: original model was built with separator=" + separatorOrig);
    }
    int gramsOrig = input.readVInt();
    if (gramsOrig != grams) {
        throw new IllegalStateException("grams=" + grams + " is incorrect: original model was built with grams=" + gramsOrig);
    }
    totTokens = input.readVLong();

    fst = new FST<Long>(input, PositiveIntOutputs.getSingleton());

    return true;
  }

  @Override
  public List<LookupResult> lookup(final CharSequence key, /* ignored */ boolean onlyMorePopular, int num) {
    try {
      return lookup(key, num);
    } catch (IOException ioe) {
      // bogus:
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public long getCount() {
        return count;
    }

  private int countGrams(BytesRef token) {
    int count = 1;
    for(int i=0;i<token.length;i++) {
      if (token.bytes[token.offset + i] == separator) {
        count++;
      }
    }

    return count;
  }

  /** Retrieve suggestions. */
  public List<LookupResult> lookup(final CharSequence key, int num) throws IOException {
    TokenStream ts = queryAnalyzer.tokenStream("", key.toString());
    try {
      TermToBytesRefAttribute termBytesAtt = ts.addAttribute(TermToBytesRefAttribute.class);
      OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
      PositionLengthAttribute posLenAtt = ts.addAttribute(PositionLengthAttribute.class);
      PositionIncrementAttribute posIncAtt = ts.addAttribute(PositionIncrementAttribute.class);
      ts.reset();

      BytesRef[] lastTokens = new BytesRef[grams];
      //System.out.println("lookup: key='" + key + "'");

      // Run full analysis, but save only the
      // last 1gram, last 2gram, etc.:
      BytesRef tokenBytes = termBytesAtt.getBytesRef();
      int maxEndOffset = -1;
      boolean sawRealToken = false;
      while(ts.incrementToken()) {
        termBytesAtt.fillBytesRef();
        sawRealToken |= tokenBytes.length > 0;
        // TODO: this is somewhat iffy; today, ShingleFilter
        // sets posLen to the gram count; maybe we should make
        // a separate dedicated att for this?
        int gramCount = posLenAtt.getPositionLength();

        assert gramCount <= grams;

        // Safety: make sure the recalculated count "agrees":
        if (countGrams(tokenBytes) != gramCount) {
          throw new IllegalArgumentException("tokens must not contain separator byte; got token=" + tokenBytes + " but gramCount=" + gramCount + " does not match recalculated count=" + countGrams(tokenBytes));
        }
        maxEndOffset = Math.max(maxEndOffset, offsetAtt.endOffset());
        lastTokens[gramCount-1] = BytesRef.deepCopyOf(tokenBytes);
      }
      ts.end();

      if (!sawRealToken) {
        throw new IllegalArgumentException("no tokens produced by analyzer, or the only tokens were empty strings");
      }

      // Carefully fill last tokens with _ tokens;
      // ShingleFilter appraently won't emit "only hole"
      // tokens:
      int endPosInc = posIncAtt.getPositionIncrement();

      // Note this will also be true if input is the empty
      // string (in which case we saw no tokens and
      // maxEndOffset is still -1), which in fact works out OK
      // because we fill the unigram with an empty BytesRef
      // below:
      boolean lastTokenEnded = offsetAtt.endOffset() > maxEndOffset || endPosInc > 0;
      //System.out.println("maxEndOffset=" + maxEndOffset + " vs " + offsetAtt.endOffset());

      if (lastTokenEnded) {
        //System.out.println("  lastTokenEnded");
        // If user hit space after the last token, then
        // "upgrade" all tokens.  This way "foo " will suggest
        // all bigrams starting w/ foo, and not any unigrams
        // starting with "foo":
        for(int i=grams-1;i>0;i--) {
          BytesRef token = lastTokens[i-1];
          if (token == null) {
            continue;
          }
          token.grow(token.length+1);
          token.bytes[token.length] = separator;
          token.length++;
          lastTokens[i] = token;
        }
        lastTokens[0] = new BytesRef();
      }

      Arc<Long> arc = new Arc<Long>();

      BytesReader bytesReader = fst.getBytesReader();

      // Try highest order models first, and if they return
      // results, return that; else, fallback:
      double backoff = 1.0;

      List<LookupResult> results = new ArrayList<LookupResult>(num);

      // We only add a given suffix once, from the highest
      // order model that saw it; for subsequent lower order
      // models we skip it:
      final Set<BytesRef> seen = new HashSet<BytesRef>();

      for(int gram=grams-1;gram>=0;gram--) {
        BytesRef token = lastTokens[gram];
        // Don't make unigram predictions from empty string:
        if (token == null || (token.length == 0 && key.length() > 0)) {
          // Input didn't have enough tokens:
          //System.out.println("  gram=" + gram + ": skip: not enough input");
          continue;
        }

        if (endPosInc > 0 && gram <= endPosInc) {
          // Skip hole-only predictions; in theory we
          // shouldn't have to do this, but we'd need to fix
          // ShingleFilter to produce only-hole tokens:
          //System.out.println("  break: only holes now");
          break;
        }

        //System.out.println("try " + (gram+1) + " gram token=" + token.utf8ToString());

        // TODO: we could add fuzziness here
        // match the prefix portion exactly
        //Pair<Long,BytesRef> prefixOutput = null;
        Long prefixOutput = null;
        try {
          prefixOutput = lookupPrefix(fst, bytesReader, token, arc);
        } catch (IOException bogus) {
          throw new RuntimeException(bogus);
        }
        //System.out.println("  prefixOutput=" + prefixOutput);

        if (prefixOutput == null) {
          // This model never saw this prefix, e.g. the
          // trigram model never saw context "purple mushroom"
          backoff *= ALPHA;
          continue;
        }

        // TODO: we could do this division at build time, and
        // bake it into the FST?

        // Denominator for computing scores from current
        // model's predictions:
        long contextCount = totTokens;

        BytesRef lastTokenFragment = null;

        for(int i=token.length-1;i>=0;i--) {
          if (token.bytes[token.offset+i] == separator) {
            BytesRef context = new BytesRef(token.bytes, token.offset, i);
            Long output = Util.get(fst, Util.toIntsRef(context, new IntsRef()));
            assert output != null;
            contextCount = decodeWeight(output);
            lastTokenFragment = new BytesRef(token.bytes, token.offset + i + 1, token.length - i - 1);
            break;
          }
        }

        final BytesRef finalLastToken;

        if (lastTokenFragment == null) {
          finalLastToken = BytesRef.deepCopyOf(token);
        } else {
          finalLastToken = BytesRef.deepCopyOf(lastTokenFragment);
        }
        assert finalLastToken.offset == 0;

        CharsRef spare = new CharsRef();

        // complete top-N
        MinResult<Long> completions[] = null;
        try {

          // Because we store multiple models in one FST
          // (1gram, 2gram, 3gram), we must restrict the
          // search so that it only considers the current
          // model.  For highest order model, this is not
          // necessary since all completions in the FST
          // must be from this model, but for lower order
          // models we have to filter out the higher order
          // ones:

          // Must do num+seen.size() for queue depth because we may
          // reject up to seen.size() paths in acceptResult():
          Util.TopNSearcher<Long> searcher = new Util.TopNSearcher<Long>(fst, num, num+seen.size(), weightComparator) {

            BytesRef scratchBytes = new BytesRef();

            @Override
            protected void addIfCompetitive(Util.FSTPath<Long> path) {
              if (path.arc.label != separator) {
                //System.out.println("    keep path: " + Util.toBytesRef(path.input, new BytesRef()).utf8ToString() + "; " + path + "; arc=" + path.arc);
                super.addIfCompetitive(path);
              } else {
                //System.out.println("    prevent path: " + Util.toBytesRef(path.input, new BytesRef()).utf8ToString() + "; " + path + "; arc=" + path.arc);
              }
            }

            @Override
            protected boolean acceptResult(IntsRef input, Long output) {
              Util.toBytesRef(input, scratchBytes);
              finalLastToken.grow(finalLastToken.length + scratchBytes.length);
              int lenSav = finalLastToken.length;
              finalLastToken.append(scratchBytes);
              //System.out.println("    accept? input='" + scratchBytes.utf8ToString() + "'; lastToken='" + finalLastToken.utf8ToString() + "'; return " + (seen.contains(finalLastToken) == false));
              boolean ret = seen.contains(finalLastToken) == false;

              finalLastToken.length = lenSav;
              return ret;
            }
          };

          // since this search is initialized with a single start node
          // it is okay to start with an empty input path here
          searcher.addStartPaths(arc, prefixOutput, true, new IntsRef());

          completions = searcher.search();
        } catch (IOException bogus) {
          throw new RuntimeException(bogus);
        }

        int prefixLength = token.length;

        BytesRef suffix = new BytesRef(8);
        //System.out.println("    " + completions.length + " completions");

        nextCompletion:
          for (MinResult<Long> completion : completions) {
            token.length = prefixLength;
            // append suffix
            Util.toBytesRef(completion.input, suffix);
            token.append(suffix);

            //System.out.println("    completion " + token.utf8ToString());

            // Skip this path if a higher-order model already
            // saw/predicted its last token:
            BytesRef lastToken = token;
            for(int i=token.length-1;i>=0;i--) {
              if (token.bytes[token.offset+i] == separator) {
                assert token.length-i-1 > 0;
                lastToken = new BytesRef(token.bytes, token.offset+i+1, token.length-i-1);
                break;
              }
            }
            if (seen.contains(lastToken)) {
              //System.out.println("      skip dup " + lastToken.utf8ToString());
              continue nextCompletion;
            }
            seen.add(BytesRef.deepCopyOf(lastToken));
            spare.grow(token.length);
            UnicodeUtil.UTF8toUTF16(token, spare);
            LookupResult result = new LookupResult(spare.toString(), (long) (Long.MAX_VALUE * backoff * ((double) decodeWeight(completion.output)) / contextCount));
            results.add(result);
            assert results.size() == seen.size();
            //System.out.println("  add result=" + result);
          }
        backoff *= ALPHA;
      }

      // change this back to Collections.sort when we switch to java 1.7 as default, which is why this works in lucene trunk without forbidden API exception
      CollectionUtil.timSort(results, new Comparator<LookupResult>() {
        @Override
        public int compare(LookupResult a, LookupResult b) {
          if (a.value > b.value) {
            return -1;
          } else if (a.value < b.value) {
            return 1;
          } else {
            // Tie break by UTF16 sort order:
            return ((String) a.key).compareTo((String) b.key);
          }
        }
      });

      if (results.size() > num) {
        results.subList(num, results.size()).clear();
      }

      return results;
    } finally {
      IOUtils.closeWhileHandlingException(ts);
    }
  }

  /** weight -> cost */
  private long encodeWeight(long ngramCount) {
    return Long.MAX_VALUE - ngramCount;
  }

  /** cost -> weight */
  //private long decodeWeight(Pair<Long,BytesRef> output) {
  private long decodeWeight(Long output) {
    assert output != null;
    return (int)(Long.MAX_VALUE - output);
  }

  // NOTE: copied from WFSTCompletionLookup & tweaked
  private Long lookupPrefix(FST<Long> fst, FST.BytesReader bytesReader,
                            BytesRef scratch, Arc<Long> arc) throws /*Bogus*/IOException {

    Long output = fst.outputs.getNoOutput();

    fst.getFirstArc(arc);

    byte[] bytes = scratch.bytes;
    int pos = scratch.offset;
    int end = pos + scratch.length;
    while (pos < end) {
      if (fst.findTargetArc(bytes[pos++] & 0xff, arc, arc, bytesReader) == null) {
        return null;
      } else {
        output = fst.outputs.add(output, arc.output);
      }
    }

    return output;
  }

  static final Comparator<Long> weightComparator = new Comparator<Long> () {
    @Override
    public int compare(Long left, Long right) {
      return left.compareTo(right);
    }
  };

  /**
   * Returns the weight associated with an input string,
   * or null if it does not exist.
   */
  public Object get(CharSequence key) {
    throw new UnsupportedOperationException();
  }


    public static class XBuilder {

        private Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, PositiveIntOutputs.getSingleton());
        private IntsRef scratchInts = new IntsRef();
        private BytesRef analyzed = new BytesRef();

        public void startTerm(BytesRef analyzed) {
            this.analyzed.copyBytes(analyzed);
        }

        public void finishTerm(long totalTermFreq) throws IOException {
            builder.add(Util.toIntsRef(analyzed, scratchInts), Long.MAX_VALUE - totalTermFreq);
        }

        public FST<Long> build() throws IOException {
            return builder.finish();
        }
    }
}
