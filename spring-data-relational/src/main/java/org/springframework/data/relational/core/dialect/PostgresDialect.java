/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.relational.core.dialect;

import lombok.RequiredArgsConstructor;

import org.springframework.data.relational.core.sql.From;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.Quoting;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.List;

/**
 * An SQL dialect for Postgres.
 *
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @since 1.1
 */
public class PostgresDialect extends AbstractDialect {

	/**
	 * Singleton instance.
	 */
	public static final PostgresDialect INSTANCE = new PostgresDialect();

	protected PostgresDialect() {}

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getLimit(long)
		 */
		@Override
		public String getLimit(long limit) {
			return "LIMIT " + limit;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getOffset(long)
		 */
		@Override
		public String getOffset(long offset) {
			return "OFFSET " + offset;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getClause(long, long)
		 */
		@Override
		public String getLimitOffset(long limit, long offset) {
			return String.format("LIMIT %d OFFSET %d", limit, offset);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getClausePosition()
		 */
		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	private final PostgresArrayColumns ARRAY_COLUMNS = new PostgresArrayColumns();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.dialect.Dialect#limit()
	 */
	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	private final PostgresLockClause LOCK_CLAUSE = new PostgresLockClause(this.getIdentifierProcessing());

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.dialect.Dialect#lock()
	 */
	@Override
	public LockClause lock() {
		return LOCK_CLAUSE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.dialect.Dialect#getArraySupport()
	 */
	@Override
	public ArrayColumns getArraySupport() {
		return ARRAY_COLUMNS;
	}

	static class PostgresLockClause implements LockClause {

		private final IdentifierProcessing identifierProcessing;

		PostgresLockClause(IdentifierProcessing identifierProcessing) {
			this.identifierProcessing = identifierProcessing;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LockClause#getLock(LockOptions)
		 */
		@Override
		public String getLock(LockOptions lockOptions) {

			List<Table> tables = lockOptions.getFrom().getTables();
			if (tables.isEmpty()) {
				return "";
			}

			String tableName = tables.get(0).getName().toSql(this.identifierProcessing);

			switch (lockOptions.getLockMode()) {

				case PESSIMISTIC_WRITE:
					return "FOR UPDATE OF " + tableName;

				case PESSIMISTIC_READ:
					return "FOR SHARE OF " + tableName;

				default:
					return "";
			}
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LockClause#getClausePosition()
		 */
		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	@RequiredArgsConstructor
	static class PostgresArrayColumns implements ArrayColumns {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.ArrayColumns#isSupported()
		 */
		@Override
		public boolean isSupported() {
			return true;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.ArrayColumns#getArrayType(java.lang.Class)
		 */
		@Override
		public Class<?> getArrayType(Class<?> userType) {

			Assert.notNull(userType, "Array component type must not be null");

			return ClassUtils.resolvePrimitiveIfNecessary(userType);
		}
	}

	@Override
	public IdentifierProcessing getIdentifierProcessing() {
		return IdentifierProcessing.create(Quoting.ANSI, LetterCasing.LOWER_CASE);
	}
}
