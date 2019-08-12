package org.elasticsearch.common.time;

import java.util.Locale;
import java.util.spi.CalendarDataProvider;

public class IsoCalendarDataProvider extends CalendarDataProvider {

    @Override
    public int getFirstDayOfWeek(Locale locale) {
        return 1;
    }

    @Override
    public int getMinimalDaysInFirstWeek(Locale locale) {
        return 4;
    }

    @Override
    public Locale[] getAvailableLocales() {
        return new Locale[]{Locale.ROOT};
    }
}
