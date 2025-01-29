pipeline {
    agent any
        tools { 
            go 'Go Version 1.23.5'
            maven 'Maven-3.8.4' 
        }
    stages {
        stage('Compile') {
            steps {
                script {
                        echo "Branch is ${BRANCH_NAME} ..."
                        
                        sh '''
                        echo "PATH = ${PATH}"
                        echo "BRANCH_NAME = ${BRANCH_NAME}"
                        go build
                        '''
                }
            }
        }
        stage ('Package') {
			when {
			    not {
			        branch 'master'
			    }
			}
            steps {
                sh '''
                jar -cvf AirportEnrichment-${BRANCH_NAME}.jar application.properties AirportEnrichment
                '''
            }
		}

        stage ('Deploy') {
			when {
			    not {
			        branch 'master'
			    }
			}
			steps {
                sh '''
                REPOSITORY="maven-releases"
                if [[ $BRANCH_NAME == *"SNAPSHOT"* ]]; then
                    REPOSITORY="maven-snapshots"
                fi
                echo "REPOSITORY = ${REPOSITORY}"

                mvn deploy:deploy-file -DgroupId=com.kerneldc -DartifactId=AirportEnrichment -Dversion=${BRANCH_NAME} -DgeneratePom=true -Dpackaging=jar -DrepositoryId=kerneldc-nexus -Durl=http://localhost:8081/repository/${REPOSITORY} -Dfile=AirportEnrichment-${BRANCH_NAME}.jar
                '''
            }
        }

    }
}