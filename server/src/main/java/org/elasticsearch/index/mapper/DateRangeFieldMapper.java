public class DateRangeFieldMapper extends FieldMapper {

    private final DateFieldMapper startFieldMapper;
    private final DateFieldMapper endFieldMapper;

    public DateRangeFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo,
                                DateFieldMapper startFieldMapper, DateFieldMapper endFieldMapper) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.startFieldMapper = startFieldMapper;
        this.endFieldMapper = endFieldMapper;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();

        if (token.equals(XContentParser.Token.START_OBJECT)) {
            String currentFieldName = null;
            Instant startDate = null;
            Instant endDate = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (currentFieldName != null) {
                    if (currentFieldName.equals("start")) {
                        startDate = Instant.parse(parser.text());
                    } else if (currentFieldName.equals("end")) {
                        endDate = Instant.parse(parser.text());
                    }
                }
            }
            if (startDate != null && endDate != null) {
                Range range = new Range(startDate, endDate);
                context.doc().add(new Field(fieldType().name(), range.toString(), fieldType()));
            }
        } else {
            throw new IllegalArgumentException("Expected START_OBJECT but got " + token);
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        boolean includeInAll = params.paramAsBoolean("include_in_all", false);

        builder.startObject("start");
        builder.field("type", startFieldMapper.contentType());
        if (includeInAll) {
            builder.field("include_in_all", true);
        }
        builder.field("format", startFieldMapper.dateTimeFormatter().pattern());
        builder.endObject();

        builder.startObject("end");
        builder.field("type", endFieldMapper.contentType());
        if (includeInAll) {
            builder.field("include_in_all", true);
        }
        builder.field("format", endFieldMapper.dateTimeFormatter().pattern());
        builder.endObject();
    }

    @Override
    protected String contentType() {
        return "date_range";
    }
}