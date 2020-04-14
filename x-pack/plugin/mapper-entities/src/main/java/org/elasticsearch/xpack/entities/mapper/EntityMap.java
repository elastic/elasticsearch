/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.entities.mapper;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EntityMap {

    public static class Builder {

        private final BytesRefHash words;
        private final Map<BytesRef, BytesRef> toEntity = new HashMap<>();
        private final Map<BytesRef, BytesRef> toSynonyms = new HashMap<>();
        private final ByteArrayDataOutput bytesWriter = new ByteArrayDataOutput();
        private final BytesRefBuilder scratch = new BytesRefBuilder();

        public Builder(BigArrays bigArrays) {
            this.words = new BytesRefHash(128, bigArrays);  // TODO pass initial capacity as well?
        }

        public Builder addEntity(BytesRef canonicalForm, BytesRef... synonyms) {
            try {
                if (synonyms.length == 0) {
                    return this;
                }
                long entityId = words.add(canonicalForm);
                if (entityId < 0) {
                    throw new IllegalArgumentException("Tried to add entity " + canonicalForm.utf8ToString() + " more than once");
                }
                scratch.grow(synonyms.length + 1);
                bytesWriter.reset(scratch.bytes());
                bytesWriter.writeVInt(synonyms.length + 1);
                bytesWriter.writeVLong(entityId);
                for (BytesRef synonym : synonyms) {
                    long synonymId = words.add(synonym);
                    if (synonymId < 0) {
                        throw new IllegalArgumentException("Tried to add synonym " + synonym.utf8ToString() + " more than once");
                    }
                    toEntity.put(synonym, canonicalForm);
                    bytesWriter.writeVLong(synonymId);
                }
                scratch.setLength(bytesWriter.getPosition());
                toSynonyms.put(canonicalForm, scratch.toBytesRef());
                for (BytesRef synonym : synonyms) {
                    toSynonyms.put(synonym, scratch.toBytesRef());
                }
                return this;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private FST<BytesRef> buildFST(Map<BytesRef, BytesRef> map) {
            ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
            org.apache.lucene.util.fst.Builder<BytesRef> builder =
                new org.apache.lucene.util.fst.Builder<>(FST.INPUT_TYPE.BYTE4, outputs);
            IntsRefBuilder scratch = new IntsRefBuilder();

            List<BytesRef> sortedKeys = map.keySet().stream().sorted().collect(Collectors.toList());
            try {
                for (BytesRef key : sortedKeys) {
                    builder.add(Util.toIntsRef(key, scratch), map.get(key));
                }
                return builder.finish();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public EntityMap build() {
            return new EntityMap(words, buildFST(toSynonyms), buildFST(toEntity));
        }

    }

    private final BytesRefHash words;
    private final FST<BytesRef> toSynonyms;
    private final FST<BytesRef> toEntities;
    private final ThreadLocal<FSTReaderState> readers;

    private static class FSTReaderState {
        final FST.BytesReader synonymsReader;
        final FST.BytesReader entitiesReader;
        final FST.Arc<BytesRef> scratchArc;
        final ByteArrayDataInput bytesReader;

        private FSTReaderState(FST.BytesReader synonymsReader, FST.BytesReader entitiesReader) {
            this.synonymsReader = synonymsReader;
            this.entitiesReader = entitiesReader;
            this.bytesReader = new ByteArrayDataInput();
            this.scratchArc = new FST.Arc<>();
        }
    }

    public EntityMap(BytesRefHash words, FST<BytesRef> toSynonyms, FST<BytesRef> toEntities) {
        this.words = words;
        this.toSynonyms = toSynonyms;
        this.toEntities = toEntities;
        this.readers = ThreadLocal.withInitial(() -> new FSTReaderState(toSynonyms.getBytesReader(), toEntities.getBytesReader()));
    }

    public BytesRefIterator findSynonyms(BytesRef in) {
        FSTReaderState readers = this.readers.get();
        BytesRef encoded = findMatch(in, toSynonyms, readers.synonymsReader, readers.scratchArc);
        if (encoded == null) {
            return null;
        }
        return decodeMatches(encoded, readers.bytesReader);
    }

    public BytesRef findCanonicalEntity(BytesRef in) {
        FSTReaderState readers = this.readers.get();
        return findMatch(in, toEntities, readers.entitiesReader, readers.scratchArc);
    }

    private BytesRef findMatch(BytesRef in, FST<BytesRef> fst, FST.BytesReader fstReader, FST.Arc<BytesRef> scratchArc) {
        try {
            BytesRef output = fst.outputs.getNoOutput();
            fst.getFirstArc(scratchArc);
            int pos = 0;
            while (pos < in.length) {
                scratchArc = fst.findTargetArc(in.bytes[in.offset + pos], scratchArc, scratchArc, fstReader);
                if (scratchArc == null) {
                    return null;
                }
                output = fst.outputs.add(output, scratchArc.output());
                pos++;
            }
            if (scratchArc.isFinal() == false) {
                return null;
            }
            return fst.outputs.add(output, scratchArc.nextFinalOutput());
        } catch (IOException e) {
            throw new UncheckedIOException(e);  // everything's in memory
        }
    }

    private BytesRefIterator decodeMatches(BytesRef matches, ByteArrayDataInput bytesReader) {
        bytesReader.reset(matches.bytes, matches.offset, matches.length);
        int count = bytesReader.readVInt();
        return new BytesRefIterator() {
            int remaining = count;
            BytesRef scratch = new BytesRef();
            @Override
            public BytesRef next() {
                if (remaining == 0)
                    return null;
                remaining--;
                long label = bytesReader.readVLong();
                return words.get(label, scratch);
            }
        };
    }
}
