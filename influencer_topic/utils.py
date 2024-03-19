class InfluencerTopicUltils:
    def _check_user_request(type):
        file = open(f'./request_{type}.txt', 'r')
        request = file.read()
        if request == '':
            request = 0

        return int(request)

    def _update_user_request(type, number=None):
        f = open(f'request_{type}.txt', "w")
        number = number + 1
        f.write(str(number))
        f.close()

    def _update_user_end_request(type, number=None):
        f = open(f'request_{type}.txt', "w")
        number = number - 1
        if number < 0:
            number = 0
        f.write(str(number))
        f.close()