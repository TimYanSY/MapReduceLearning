from mrjob.job import MRJob

# EZE00100082,18001224,TMIN,6,,,E
class MRMinTempratureByLocation(MRJob):
    def mapper(self, key, line):
        (location, _date, tempType, temprature, _temp1, _temp2, _temp3, _temp4) = line.split(',')
        if tempType == 'TMIN':
            yield location, temprature
    def reducer(self, location, temprature):
        yield location, min(temprature)

if __name__ == '__main__':
    MRMinTempratureByLocation.run()