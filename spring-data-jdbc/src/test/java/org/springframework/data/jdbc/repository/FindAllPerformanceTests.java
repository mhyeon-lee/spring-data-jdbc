package org.springframework.data.jdbc.repository;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@ContextConfiguration
@ActiveProfiles("hsql")
public class FindAllPerformanceTests {

	@Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true)
	static class Config {

		@Autowired
		JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return FindAllPerformanceTests.class;
		}
	}

	@ClassRule
	public static final SpringClassRule classRule = new SpringClassRule();
	@Rule
	public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired
	private PersonRepository repository;

	@Before
	public void setUp() {
		if (repository.count() == 0) {
			List<Person> persons = new ArrayList<>();
			for (int i = 0; i < 200; i++) {
				persons.add(new Person(
					null, "firstName" + i, "lastName" + i, "ssn" + i,
					i, Instant.now().minus(i, ChronoUnit.DAYS), i > 19, "address" + i, "addressDetail" + i,
					"country" + i, "state" + i, "zipCode" + i, "job" + i, "company" + i, "email" + i));
			}

			repository.saveAll(persons);
		}
	}

	@Test
	public void execute() throws InterruptedException {
		// warm up
		for (int i = 0; i < 5; i++) {
			run(1000);
		}

		long elapsed1 = run(1000);
		System.out.println("Elapsed1 : " + elapsed1);

		long elapsed2 = run(1000);
		System.out.println("Elapsed2 : " + elapsed2);

		long elapsed3 = run(1000);
		System.out.println("Elapsed3 : " + elapsed3);

		long elapsed4 = run(1000);
		System.out.println("Elapsed4 : " + elapsed4);

		long elapsed5 = run(1000);
		System.out.println("Elapsed5 : " + elapsed5);

		System.out.println("Average : " + (elapsed1 + elapsed2 + elapsed3 + elapsed4 + elapsed5) / 5);
	}

	private long run(int count) throws InterruptedException {
		CountDownLatch startLatch = new CountDownLatch(count + 1);
		CountDownLatch doneLatch = new CountDownLatch(count);

		for (int i = 0; i < count; i++) {
			this.execute(startLatch, doneLatch);
		}

		Thread.sleep(1000);

		long started = System.currentTimeMillis();
		startLatch.countDown();

		doneLatch.await();
		return System.currentTimeMillis() - started;
	}

	private void execute(CountDownLatch startLatch, CountDownLatch doneLatch) {
		new Thread(() -> {
			try {
				startLatch.countDown();
				startLatch.await();

				this.repository.findAll();

			} catch (InterruptedException ignored) {
			} finally {
				doneLatch.countDown();
			}
		}).start();
	}

	@Getter
	@AllArgsConstructor
	static class Person {
		@Id
		private Long id;
		private String firstName;
		private String lastName;
		private String ssn;
		private int age;
		private Instant birth;
		private boolean adult;
		private String address;
		private String addressDetail;
		private String country;
		private String state;
		private String zipCode;
		private String job;
		private String company;
		private String email;
	}

	interface PersonRepository extends CrudRepository<Person, Long> {}
}
