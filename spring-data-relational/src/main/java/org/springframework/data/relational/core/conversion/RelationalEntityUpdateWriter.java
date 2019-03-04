/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.core.conversion;

import java.util.List;

import org.springframework.data.convert.EntityWriter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Converts an aggregate represented by its root into an {@link AggregateChange}. Does not perform any isNew check.
 *
 * @author Thomas Lang
 * @author Jens Schauder
 * @since 1.1
 */
public class RelationalEntityUpdateWriter implements EntityWriter<Object, AggregateChange<?>> {

	private final RelationalMappingContext context;

	public RelationalEntityUpdateWriter(RelationalMappingContext context) {
		this.context = context;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityWriter#save(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void write(Object root, AggregateChange<?> aggregateChange) {

		List<DbAction<?>> actions = new WritingContext(context, root, aggregateChange).update();
		actions.forEach(aggregateChange::addAction);
	}
}