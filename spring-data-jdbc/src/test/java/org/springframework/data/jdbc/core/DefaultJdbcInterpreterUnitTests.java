/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.conversion.DbAction.Insert;
import org.springframework.data.relational.core.conversion.DbAction.InsertRoot;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.domain.Identifier;

/**
 * Unit tests for {@link DefaultJdbcInterpreter}
 *
 * @author Jens Schauder
 */
public class DefaultJdbcInterpreterUnitTests {

	static final long CONTAINER_ID = 23L;
	static final String BACK_REFERENCE = "container";

	RelationalMappingContext context = new JdbcMappingContext();

	DataAccessStrategy dataAccessStrategy = mock(DataAccessStrategy.class);
	DefaultJdbcInterpreter interpreter = new DefaultJdbcInterpreter(context, dataAccessStrategy);

	Container container = new Container();
	Element element = new Element();

	InsertRoot<Container> containerInsert = new InsertRoot<>(container);
	Insert<?> elementInsert = new Insert<>(element, PropertyPathTestingUtils.toPath("element", Container.class, context),
			containerInsert);
	Insert<?> element1Insert = new Insert<>(element, PropertyPathTestingUtils.toPath("element.element1", Container.class, context),
			elementInsert);

	@Test // DATAJDBC-145
	public void insertDoesHonourNamingStrategyForBackReference() {

		container.id = CONTAINER_ID;
		containerInsert.setGeneratedId(CONTAINER_ID);

		interpreter.interpret(elementInsert);

		ArgumentCaptor<Identifier> argumentCaptor = ArgumentCaptor.forClass(Identifier.class);
		verify(dataAccessStrategy).insert(eq(element), eq(Element.class), argumentCaptor.capture());

		assertThat(argumentCaptor.getValue().getParts()) //
				.extracting("name", "value", "targetType") //
				.containsExactly(tuple(BACK_REFERENCE, CONTAINER_ID, Long.class));
	}

	@Test // DATAJDBC-251
	public void idOfParentGetsPassedOnAsAdditionalParameterIfNoIdGotGenerated() {

		container.id = CONTAINER_ID;

		interpreter.interpret(elementInsert);

		ArgumentCaptor<Identifier> argumentCaptor = ArgumentCaptor.forClass(Identifier.class);
		verify(dataAccessStrategy).insert(eq(element), eq(Element.class), argumentCaptor.capture());

		assertThat(argumentCaptor.getValue().getParts()) //
				.extracting("name", "value", "targetType") //
				.containsExactly(tuple(BACK_REFERENCE, CONTAINER_ID, Long.class));
	}

	@Test // DATAJDBC-251
	public void generatedIdOfParentGetsPassedOnAsAdditionalParameter() {

		containerInsert.setGeneratedId(CONTAINER_ID);

		interpreter.interpret(elementInsert);

		ArgumentCaptor<Identifier> argumentCaptor = ArgumentCaptor.forClass(Identifier.class);
		verify(dataAccessStrategy).insert(eq(element), eq(Element.class), argumentCaptor.capture());

		assertThat(argumentCaptor.getValue().getParts()) //
				.extracting("name", "value", "targetType") //
				.containsExactly(tuple(BACK_REFERENCE, CONTAINER_ID, Long.class));
	}

	@Test // DATAJDBC-359
	public void generatedIdOfParentsParentGetsPassedOnAsAdditionalParameter() {

		containerInsert.setGeneratedId(CONTAINER_ID);

		interpreter.interpret(element1Insert);

		ArgumentCaptor<Identifier> argumentCaptor = ArgumentCaptor.forClass(Identifier.class);
		verify(dataAccessStrategy).insert(eq(element), eq(Element.class), argumentCaptor.capture());

		assertThat(argumentCaptor.getValue().getParts()) //
				.extracting("name", "value", "targetType") //
				.containsExactly(tuple(BACK_REFERENCE, CONTAINER_ID, Long.class));
	}

	@SuppressWarnings("unused")
	static class Container {

		@Id Long id;

		Element element;
	}

	@SuppressWarnings("unused")
	static class Element {
		Element1 element1;
	}

	static class Element1 {
	}
}