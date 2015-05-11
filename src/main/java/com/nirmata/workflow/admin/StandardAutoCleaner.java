/**
 * Copyright 2014 Nirmata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nirmata.workflow.admin;

import org.joda.time.Duration;
import org.joda.time.LocalDateTime;

import static org.joda.time.DateTimeZone.UTC;

/**
 * Default auto cleaner. Cleans if the run is completed and was completed past the given minimum age
 */
public class StandardAutoCleaner implements AutoCleaner
{
    private final Duration minAge;

    public StandardAutoCleaner(Duration minAge)
    {
        this.minAge = minAge;
    }

    @Override
    public boolean canBeCleaned(RunInfo runInfo)
    {
        if ( runInfo.isComplete() )
        {
            LocalDateTime nowUtc = LocalDateTime.now(UTC);
            Duration durationSinceCompletion = new Duration(runInfo.getCompletionTimeUtc().toDateTime(UTC), nowUtc.toDateTime(UTC));
            if ( durationSinceCompletion.compareTo(minAge) >= 0 )
            {
                return true;
            }
        }
        return false;
    }
}
