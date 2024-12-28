package hello.springtx.exception;

import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
public class RollbackTest {

    @Autowired
    RollbackService rollbackService;

    @Test
    void runTimeException() {
        Assertions.assertThatThrownBy(() -> rollbackService.runTimeException()).isInstanceOf(RuntimeException.class);
    }

    @Test
    void checkedException() {
        Assertions.assertThatThrownBy(() -> rollbackService.checkedException()).isInstanceOf(MyNewException.class);
    }

    @Test
    void rollbackFor() {
        Assertions.assertThatThrownBy(() -> rollbackService.rollbackFor()).isInstanceOf(MyNewException.class);
    }

    @TestConfiguration
    static class RollbackTestConfiguration {

        @Bean
        RollbackService rollbackService() {
            return new RollbackService();
        }
    }

    @Slf4j
    static class RollbackService {

        //런타임 예외 발생: 롤백
        @Transactional
        public void runTimeException() {
            log.info("call runtime Exception");
            throw new RuntimeException();
        }

        // 체크 예외 발생: 커밋
        @Transactional
        public void checkedException() throws MyNewException {
            log.info("call checkedException");
            throw new MyNewException();
        }

        // 체크 예외 rollbackFor 지정: 롤백
        @Transactional(rollbackFor = MyNewException.class)
        public void rollbackFor() throws MyNewException {
            log.info("call checkedException");
            throw new MyNewException();
        }
    }

    static class MyNewException extends Exception {}
}
