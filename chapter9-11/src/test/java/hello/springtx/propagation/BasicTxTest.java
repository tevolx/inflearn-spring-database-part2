package hello.springtx.propagation;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.*;

@Slf4j
@SpringBootTest
public class BasicTxTest {

    @Autowired
    PlatformTransactionManager transactionManager;

    @TestConfiguration
    static class Config {
        @Bean
        public PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }
    }

    @Test
    void commit() {
        log.info("begin transaction");
        TransactionStatus status = transactionManager.getTransaction(new DefaultTransactionDefinition());

        log.info("start transaction commit");
        transactionManager.commit(status);
        log.info("end transaction commit");
    }

    @Test
    void rollback() {
        log.info("begin transaction");
        TransactionStatus status = transactionManager.getTransaction(new DefaultTransactionDefinition());

        log.info("start transaction rollback");
        transactionManager.rollback(status);
        log.info("end transaction rollback");
    }

    @Test
    void doubleCommit() {
        log.info("begin transaction1");
        TransactionStatus tx1 = transactionManager.getTransaction(new DefaultTransactionDefinition());
        log.info("start transaction1 commit");
        transactionManager.commit(tx1);

        log.info("begin transaction2");
        TransactionStatus tx2 = transactionManager.getTransaction(new DefaultTransactionDefinition());
        log.info("start transaction2 commit");
        transactionManager.commit(tx2);
    }

    @Test
    void double_commit_rollback() {
        log.info("begin transaction1");
        TransactionStatus tx1 = transactionManager.getTransaction(new DefaultTransactionDefinition());
        log.info("start transaction1 commit");
        transactionManager.commit(tx1);

        log.info("begin transaction2");
        TransactionStatus tx2 = transactionManager.getTransaction(new DefaultTransactionDefinition());
        log.info("start transaction2 rollback");
        transactionManager.rollback(tx2);
    }

    @Test
    void inner_commit() {
        log.info("외부 트랜잭션 시작");
        TransactionStatus outer = transactionManager.getTransaction(new DefaultTransactionDefinition());
        log.info("outer.isNewTransaction()={}", outer.isNewTransaction());

        log.info("내부 트랜잭션 시작");
        TransactionStatus inner = transactionManager.getTransaction(new DefaultTransactionDefinition());
        log.info("outer.isNewTransaction()={}", inner.isNewTransaction());
        log.info("내부 트랜잭션 커밋");
        transactionManager.commit(inner);

        log.info("외부 트랜잭션 커밋");
        transactionManager.commit(outer);
    }

    @Test
    void outer_rollback() {
        log.info("외부 트랜잭션 시작");
        TransactionStatus outer = transactionManager.getTransaction(new DefaultTransactionDefinition());

        log.info("내부 트랜잭션 시작");
        TransactionStatus inner = transactionManager.getTransaction(new DefaultTransactionDefinition());
        log.info("내부 트랜잭션 커밋");
        transactionManager.commit(inner);

        log.info("외부 트랜잭션 롤백");
        transactionManager.rollback(outer);
    }

    @Test
    void inner_rollback() {
        log.info("외부 트랜잭션 시작");
        TransactionStatus outer = transactionManager.getTransaction(new DefaultTransactionDefinition());

        log.info("내부 트랜잭션 시작");
        TransactionStatus inner = transactionManager.getTransaction(new DefaultTransactionDefinition());
        log.info("내부 트랜잭션 롤백");
        transactionManager.rollback(inner); // rollback-only 표시

        log.info("외부 트랜잭션 커밋");
        assertThatThrownBy(() ->  transactionManager.commit(outer)).isInstanceOf(UnexpectedRollbackException.class);
    }

    @Test
    void inner_rollback_requires_new() {
        log.info("외부 트랜잭션 시작");
        TransactionStatus outer = transactionManager.getTransaction(new DefaultTransactionDefinition());
        log.info("outer.isNewTransaction()={}", outer.isNewTransaction());

        log.info("내부 트랜잭션 시작");
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        TransactionStatus inner = transactionManager.getTransaction(definition);
        log.info("inner.isNewTransaction()={}", inner.isNewTransaction());

        log.info("내부 트랜잭션 롤백");
        transactionManager.rollback(inner);

        log.info("외부 트랜잭션 커밋");
        transactionManager.commit(outer);
    }
}
