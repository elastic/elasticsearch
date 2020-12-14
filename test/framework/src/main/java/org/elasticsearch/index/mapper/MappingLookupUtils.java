package org.elasticsearch.index.mapper;

import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldMapper.CopyTo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MappingLookupUtils {
    public static MappingLookup fromTypes(MappedFieldType... types) {
        return fromTypes(Arrays.stream(types));
    }
    
    public static MappingLookup fromTypes(Stream<MappedFieldType> types) {
        List<FieldMapper> mappers = types.map(MappingLookupUtils::mockFieldMapper).collect(toList());
        //  Alias <name>-alias to <name> so we can test aliases
        return new MappingLookup(
            "_doc",
            mappers,
            List.of(),
            List.of(),
            List.of(),
            0,
            souceToParse -> null,
            w -> w.writeString("test" + mappers.hashCode())
        );
    }

    public static FieldMapper mockFieldMapper(MappedFieldType type) {
        FieldMapper mapper = mock(FieldMapper.class);
        when(mapper.fieldType()).thenReturn(type);
        when(mapper.name()).thenReturn(type.name());
        when(mapper.copyTo()).thenReturn(CopyTo.empty());
        Map<String, NamedAnalyzer> indexAnalyzers = Map.of();
        if (type.getTextSearchInfo() != TextSearchInfo.NONE) {
            indexAnalyzers = Map.of(mapper.name(), type.getTextSearchInfo().getSearchAnalyzer());
        }
        when(mapper.indexAnalyzers()).thenReturn(indexAnalyzers);
        return mapper;
    }
}
